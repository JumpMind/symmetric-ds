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

import org.jumpmind.symmetric.common.logging.ILog;
import org.jumpmind.symmetric.model.DataRef;
import org.jumpmind.symmetric.service.IDataService;
import org.jumpmind.symmetric.service.ISqlProvider;

/**
 * This class is responsible for reading data for the purpose of routing. It
 * reads ahead and tries to keep a blocking queue populated for other threads to
 * process.
 */
public class DataRefRouteReader extends AbstractDataToRouteReader {

    public static final String SELECT_DATA_TO_BATCH_SQL = "selectDataToBatchSql";

    public DataRefRouteReader(ILog log, ISqlProvider sqlProvider, ChannelRouterContext context,
            IDataService dataService) {
        super(log, sqlProvider, context, dataService);
    }

    @Override
    protected PreparedStatement prepareStatment(Connection c) throws SQLException {
        DataRef dataRef = dataService.getDataRef();
        String channelId = context.getChannel().getChannelId();
        PreparedStatement ps = c.prepareStatement(getSql(SELECT_DATA_TO_BATCH_SQL, context.getChannel().getChannel()),
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        ps.setQueryTimeout(jdbcTemplate.getQueryTimeout());
        ps.setFetchSize(jdbcTemplate.getFetchSize());
        ps.setString(1, channelId);
        ps.setLong(2, dataRef.getRefDataId());
        return ps;
    }

}