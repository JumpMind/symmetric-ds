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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.List;

import javax.sql.DataSource;

import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.model.DataGap;
import org.jumpmind.symmetric.service.IDataService;
import org.jumpmind.symmetric.service.ISqlProvider;
import org.jumpmind.symmetric.util.AppUtils;

/**
 * This class is responsible for reading data for the purpose of routing. It
 * reads ahead and tries to keep a blocking queue populated for other threads to
 * process.
 */
public class DataGapRouteReader extends AbstractDataToRouteReader {

    private static final String SELECT_DATA_USING_GAPS_SQL = "selectDataUsingGapsSql";

    public DataGapRouteReader(DataSource dataSource, int queryTimeout, int maxQueueSize,
            ISqlProvider sqlProvider, int fetchSize, ChannelRouterContext context, IDataService dataService, boolean requiresAutoCommitFalse, IDbDialect dbDialect) {
        super(dataSource, queryTimeout, maxQueueSize, sqlProvider, fetchSize, context, dataService, requiresAutoCommitFalse, dbDialect);
    }
    
    @Override
    protected PreparedStatement prepareSecondaryStatement(Connection c) throws SQLException {
        List<DataGap> gaps = dataService.findDataGaps();
        DataGap ref = new DataGap(0, DataGap.OPEN_END_ID, new Date());
        if (gaps != null && gaps.size() > 0) {
           ref = gaps.get(0);
        }
        String channelId = context.getChannel().getChannelId();
        PreparedStatement ps = c.prepareStatement(getSql(DataRefRouteReader.SELECT_DATA_TO_BATCH_SQL, context.getChannel().getChannel()),
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        ps.setQueryTimeout(queryTimeout);
        ps.setFetchSize(fetchSize);
        ps.setString(1, channelId);
        ps.setLong(2, ref.getStartId());
        return ps;
    }

    @Override
    protected PreparedStatement prepareStatment(Connection c) throws SQLException {
        String channelId = context.getChannel().getChannelId();
        List<DataGap> gaps = dataService.findDataGaps();
        StringBuilder gapSqlClause = new StringBuilder();
        if (gaps.size() > 0) {
            for (DataGap dataGap : gaps) {
                if (dataGap.getEndId() == DataGap.OPEN_END_ID) {
                    gapSqlClause.append(" d.data_id >= ? ");
                } else {
                    gapSqlClause.append(" (d.data_id >= ? and d.data_id <= ?) or ");
                }
            }
        } else {
            gapSqlClause.append("1=1");
        }
        String sql = AppUtils.replace("gap.clause", gapSqlClause.toString(), getSql(SELECT_DATA_USING_GAPS_SQL, context.getChannel().getChannel()));
        PreparedStatement ps = c.prepareStatement(sql,
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        ps.setQueryTimeout(queryTimeout);
        ps.setFetchSize(fetchSize);
        int index = 1;
        ps.setString(index++, channelId);        
        if (gaps.size() > 0) {
            for (DataGap dataGap : gaps) {
                if (dataGap.getEndId() == DataGap.OPEN_END_ID) {
                    ps.setLong(index++, dataGap.getStartId());
                } else {
                    ps.setLong(index++, dataGap.getStartId());
                    ps.setLong(index++, dataGap.getEndId());
                }
            }
        } 
        
        return ps;
    }

}