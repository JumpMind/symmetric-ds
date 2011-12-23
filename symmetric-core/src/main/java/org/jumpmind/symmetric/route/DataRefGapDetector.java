/*
 * Licensed to JumpMind Inc under one or more contributor 
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding 
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU Lesser General Public License (the
 * "License"); you may not use this file except in compliance
 * with the License. 
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see           
 * <http://www.gnu.org/licenses/>.
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.  */

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

/**
 * 
 */
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
        final int dataIdIncrementBy = parameterService.getInt(ParameterConstants.DATA_ID_INCREMENT_BY);
        long lastDataId = (Long) jdbcTemplate.query(sqlProvider.getSql("selectDistinctDataIdFromDataEventSql"),
                new Object[] { ref.getRefDataId() }, new int[] { Types.NUMERIC },
                new ResultSetExtractor<Long>() {
                    public Long extractData(ResultSet rs) throws SQLException, DataAccessException {
                        long lastDataId = ref.getRefDataId();
                        while (rs.next()) {
                            long dataId = rs.getLong(1);
                            if (lastDataId == -1 || lastDataId + dataIdIncrementBy == dataId
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