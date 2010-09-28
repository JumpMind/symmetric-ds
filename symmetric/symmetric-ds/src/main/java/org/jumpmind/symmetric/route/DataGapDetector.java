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
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.common.logging.ILog;
import org.jumpmind.symmetric.common.logging.LogFactory;
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.model.DataGap;
import org.jumpmind.symmetric.service.IDataService;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.service.ISqlProvider;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;

/**
 * @author Chris Henson <chenson42@users.sourceforge.net>
 */
public class DataGapDetector implements IDataToRouteGapDetector {

    final ILog log = LogFactory.getLog(getClass());

    private IDataService dataService;

    private IParameterService parameterService;

    private JdbcTemplate jdbcTemplate;

    private IDbDialect dbDialect;

    private ISqlProvider sqlProvider;

    public DataGapDetector(IDataService dataService, IParameterService parameterService,
            JdbcTemplate jdbcTemplate, IDbDialect dbDialect, ISqlProvider sqlProvider) {
        this.dataService = dataService;
        this.parameterService = parameterService;
        this.jdbcTemplate = jdbcTemplate;
        this.dbDialect = dbDialect;
        this.sqlProvider = sqlProvider;
    }
    
    public void afterRouting() {
    }

    /**
     * Always make sure sym_data_gap is up to date to make sure that we don't dual route data.
     */
    public void beforeRouting() {        
        long ts = System.currentTimeMillis();
        List<DataGap> gaps = dataService.findDataGaps();
        long lastDataId = -1;
        final int dataIdIncrementBy = parameterService.getInt(ParameterConstants.DATA_ID_INCREMENT_BY);
        for (final DataGap dataGap : gaps) {
            
            String sql = sqlProvider.getSql("selectDistinctDataIdFromDataEventUsingGapsSql");
            Object[] params = new Object[] {dataGap.getStartId(), dataGap.getEndId()};
            if (dataGap.getEndId() == DataGap.OPEN_END_ID) {
                sql = sqlProvider.getSql("selectDistinctDataIdFromDataEventSql");
                params = new Object[] {dataGap.getStartId()-1};                
            }
            
            lastDataId = jdbcTemplate.query(
                    sql, params,
                    new ResultSetExtractor<Long>() {
                        public Long extractData(ResultSet rs) throws SQLException,
                                DataAccessException {
                            List<DataGap> newGaps = new ArrayList<DataGap>();
                            boolean foundAtLeastOneDataId = false;
                            long lastDataId = -1;
                            while (rs.next()) {
                                foundAtLeastOneDataId = true;
                                long dataId = rs.getLong(1);
                                if (lastDataId != -1 && lastDataId + dataIdIncrementBy != dataId
                                        && lastDataId != dataId) {
                                    // found a gap
                                    newGaps.add(new DataGap(lastDataId+1, dataId-1));
                                } 
                                lastDataId = dataId;
                            }

                            if (foundAtLeastOneDataId) {
                                dataService.updateDataGap(dataGap, DataGap.STATUS.FL);
                            } else {
                                if (dataGap.getEndId() != DataGap.OPEN_END_ID
                                        && dataService.countDataInRange(dataGap.getStartId() - 1,
                                                dataGap.getEndId() + 1) == 0) {
                                    if (dbDialect.supportsTransactionViews()) {
                                        Date createTime = dataService
                                        .findCreateTimeOfData(
                                                dataGap.getEndId() + 1);
                                        if (createTime != null && !dbDialect
                                                .areDatabaseTransactionsPendingSince(createTime.getTime() + 5000)) {
                                            if (dataService.countDataInRange(dataGap.getStartId() - 1,
                                                    dataGap.getEndId() + 1) == 0) {
                                                log.info("RouterSkippingDataIdsNoTransactions",
                                                        dataGap.getStartId(), dataGap.getEndId());
                                                dataService.updateDataGap(dataGap, DataGap.STATUS.SK);
                                            }
                                        }
                                    } else if (isDataGapExpired(dataGap.getEndId() + 1)) {
                                        log.info("RouterSkippingDataIdsGapExpired",
                                                dataGap.getStartId(), dataGap.getEndId());
                                        dataService.updateDataGap(dataGap, DataGap.STATUS.SK);
                                    }
                                }
                            }

                            for (DataGap dataGap : newGaps) {
                                dataService.insertDataGap(dataGap);
                            }

                            return lastDataId;
                        }

                    });
        }
        
        if (lastDataId > 0) {
            dataService.insertDataGap(new DataGap(lastDataId + 1, DataGap.OPEN_END_ID));
        }

        long updateTimeInMs = System.currentTimeMillis() - ts;
        if (updateTimeInMs > 10000) {
            log.info("RoutedGapDetectionTime", updateTimeInMs);
        }

    }

    protected boolean isDataGapExpired(long dataId) {
        long gapTimoutInMs = parameterService
                .getLong(ParameterConstants.ROUTING_STALE_DATA_ID_GAP_TIME);
        Date createTime = dataService.findCreateTimeOfData(dataId);
        if (createTime != null && System.currentTimeMillis() - createTime.getTime() > gapTimoutInMs) {
            return true;
        } else {
            return false;
        }
    }

}