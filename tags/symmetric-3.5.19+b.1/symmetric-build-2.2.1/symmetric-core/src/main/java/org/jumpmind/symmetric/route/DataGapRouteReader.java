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

import javax.sql.DataSource;

import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.model.DataGap;
import org.jumpmind.symmetric.service.IDataService;
import org.jumpmind.symmetric.service.ISqlProvider;

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
    protected PreparedStatement prepareStatment(Connection c) throws SQLException {
        String channelId = context.getChannel().getChannelId();
        String sql = getSql(SELECT_DATA_USING_GAPS_SQL, context.getChannel().getChannel());
        PreparedStatement ps = c.prepareStatement(sql,
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        ps.setQueryTimeout(queryTimeout);
        ps.setFetchSize(fetchSize);
        ps.setString(1, DataGap.Status.GP.name());
        ps.setString(2, channelId);        
        return ps;
    }

}