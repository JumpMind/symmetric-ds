/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Chris Henson <chenson42@users.sourceforge.net>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see
 * <http://www.gnu.org/licenses/>.
 */
package org.jumpmind.symmetric.route;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Date;

import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.common.logging.ILog;
import org.jumpmind.symmetric.common.logging.LogFactory;
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.model.DataRef;
import org.jumpmind.symmetric.service.IDataService;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.service.ISqlProvider;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;

public class DataRefGapDetector implements IDataToRouteGapDetector {

    final ILog log = LogFactory.getLog(getClass());
    
    private IDataService dataService;
    
    private IParameterService parameterService;
    
    private JdbcTemplate jdbcTemplate;
    
    private IDbDialect dbDialect;
    
    private ISqlProvider sqlProvider;
    
    public DataRefGapDetector(IDataService dataService, IParameterService parameterService,
            JdbcTemplate jdbcTemplate, IDbDialect dbDialect, ISqlProvider sqlProvider) {
        this.dataService = dataService;
        this.parameterService = parameterService;
        this.jdbcTemplate = jdbcTemplate;
        this.dbDialect = dbDialect;
        this.sqlProvider = sqlProvider;
    }
    
    public void beforeRouting() {
    }

    public void afterRouting() {
        // reselect the DataRef just in case somebody updated it manually during
        // routing
        final DataRef ref = dataService.getDataRef();
        long ts = System.currentTimeMillis();
        long lastDataId = (Long) jdbcTemplate.query(sqlProvider.getSql("selectDistinctDataIdFromDataEventSql"),
                new Object[] { ref.getRefDataId() }, new int[] { Types.INTEGER },
                new ResultSetExtractor<Long>() {
                    public Long extractData(ResultSet rs) throws SQLException, DataAccessException {
                        long lastDataId = ref.getRefDataId();
                        while (rs.next()) {
                            long dataId = rs.getLong(1);
                            if (lastDataId == -1 || lastDataId + 1 == dataId
                                    || lastDataId == dataId) {
                                lastDataId = dataId;
                            } else {
                                if (dataService.countDataInRange(lastDataId, dataId) == 0) {
                                    if (dbDialect.supportsTransactionViews()) {
                                        if (!dbDialect
                                                    .areDatabaseTransactionsPendingSince(dataService
                                                            .findCreateTimeOfData(dataId).getTime() + 5000)) {
                                            if (dataService.countDataInRange(lastDataId, dataId) == 0) {
                                                log.info("RouterSkippingDataIdsNoTransactions", lastDataId, dataId);
                                                lastDataId = dataId;
                                            }
                                        }
                                    } else if (isDataGapExpired(dataId)) {
                                        log.info("RouterSkippingDataIdsGapExpired", lastDataId,
                                                dataId);
                                        lastDataId = dataId;
                                    } else {
                                        break;
                                    }
                                } else {
                                    // detected a gap!
                                    break;
                                }
                            }
                        }
                        return lastDataId;
                    }
                });
        long updateTimeInMs = System.currentTimeMillis() - ts;
        if (updateTimeInMs > 10000) {
            log.info("RoutedGapDetectionTime", updateTimeInMs);
        }
        if (ref.getRefDataId() != lastDataId) {
            dataService.saveDataRef(new DataRef(lastDataId, new Date()));
        }
    }

    protected boolean isDataGapExpired(long dataId) {
        long gapTimoutInMs = parameterService
                .getLong(ParameterConstants.ROUTING_STALE_DATA_ID_GAP_TIME);
        Date createTime = dataService.findCreateTimeOfEvent(dataId);
        if (createTime != null && System.currentTimeMillis() - createTime.getTime() > gapTimoutInMs) {
            return true;
        } else {
            return false;
        }
    }


}
