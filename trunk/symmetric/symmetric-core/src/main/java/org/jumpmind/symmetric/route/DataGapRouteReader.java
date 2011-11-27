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
 * under the License. 
 */
package org.jumpmind.symmetric.route;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.common.logging.ILog;
import org.jumpmind.symmetric.model.DataGap;
import org.jumpmind.symmetric.service.IDataService;
import org.jumpmind.symmetric.service.ISqlProvider;
import org.jumpmind.util.FormatUtils;

/**
 * This class is responsible for reading data for the purpose of routing. It
 * reads ahead and tries to keep a blocking queue populated for another thread
 * to process.
 */
public class DataGapRouteReader extends AbstractDataToRouteReader {

    private static final String SELECT_DATA_USING_GAPS_SQL = "selectDataUsingGapsSql";
    
    protected List<DataGap> dataGaps;
    
    protected DataGap currentGap;

    public DataGapRouteReader(ILog log, ISqlProvider sqlProvider, ChannelRouterContext context,
            IDataService dataService) {
        super(log, sqlProvider, context, dataService);
    }
    
    @Override
    protected PreparedStatement prepareStatment(Connection c) throws SQLException {
        int numberOfGapsToQualify = dataService.getParameterService().getInt(
                ParameterConstants.ROUTING_MAX_GAPS_TO_QUALIFY_IN_SQL, 100);
        
        this.dataGaps = dataService.findDataGaps();

        String channelId = context.getChannel().getChannelId();
        String sql = qualifyUsingDataGaps(dataGaps, numberOfGapsToQualify,
                getSql(SELECT_DATA_USING_GAPS_SQL, context.getChannel().getChannel()));
        PreparedStatement ps = c.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY);
        ps.setQueryTimeout(jdbcTemplate.getQueryTimeout());
        ps.setFetchSize(jdbcTemplate.getFetchSize());
        ps.setString(1, channelId);
        for (int i = 0; i < numberOfGapsToQualify && i < dataGaps.size(); i++) {          
            DataGap gap = dataGaps.get(i);
            ps.setLong(i*2 + 2, gap.getStartId());            
            if ((i+1) == numberOfGapsToQualify && (i+1) < dataGaps.size()) {
                // there were more gaps than we are going to use in the SQL.  use
                // the last gap as the end data id for the last range
                ps.setLong(i*2 + 3, dataGaps.get(dataGaps.size()-1).getEndId());
            } else {
                ps.setLong(i*2 + 3, gap.getEndId());
            }
        }
        
        this.currentGap = dataGaps.remove(0);
        return ps;
    }

    protected String qualifyUsingDataGaps(List<DataGap> dataGaps, int numberOfGapsToQualify,
            String sql) {
        StringBuilder gapClause = new StringBuilder();
        for (int i = 0; i < numberOfGapsToQualify && i < dataGaps.size(); i++) {
            if (i==0) {
                gapClause.append(" and (");
            } else {
                gapClause.append(" or ");
            }
            gapClause.append("(d.data_id between ? and ?)");
        }
        gapClause.append(")");
        return FormatUtils.replace("dataRange", gapClause.toString(), sql);
    }
    
    @Override
    protected boolean process(ResultSet rs) throws SQLException {
        long dataId = rs.getLong(1);
        if (currentGap != null && dataId >= currentGap.getStartId()) {
            if (dataId <= currentGap.getEndId()) {
                return true;
            } else {
                // past current gap. move to next gap
                if (dataGaps.size() > 0) {
                    currentGap = dataGaps.remove(0);
                    return process(rs);
                } else {
                    currentGap = null;
                    return false;
                }
            }
        } else {
            return false;
        }
    }


}