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

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.model.DataMetaData;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.TriggerRouter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.SingleColumnRowMapper;
import org.springframework.jdbc.core.simple.SimpleJdbcTemplate;

/**
 * This data router is invoked when the router_type is 'subselect'. The router_expression is always a SQL expression
 * that is used to find the list of nodes a row of data will be routed to. This router should never be used for high throughput
 * tables because it makes a call back to the database for each row that is routed.
 * <P/>
 * The query that is used to select the nodes is as follows:
 * <P/>
 * <code>
 * select c.node_id from $[sym.sync.table.prefix]_node c where c.node_group_id=:NODE_GROUP_ID and c.sync_enabled=1 and ...
 * </code>
 * <P/>
 * The SQL expression designated in the router_expression is appended to the above SQL statement.  Current and old column values can be
 * passed into the sub select expression.  For example, say you had an EMPLOYEE table and a PASSWORD table.  When the password changes you want
 * to route the password to the HOME_STORE that is stored on the EMPLOYEE table.  The sub select expression might look like:
 * <P/>
 * <code>
 * c.external_id in (select home_store from employee where employee_id in (:EMPLOYEE_ID, :OLD_EMPLOYEE_ID))
 * </code>
 *
 * 
 */
public class SubSelectDataRouter extends AbstractDataRouter {

    private String sql;

    private JdbcTemplate jdbcTemplate;

    private IDbDialect dbDialect;

    public Set<String> routeToNodes(IRouterContext routingContext, DataMetaData dataMetaData, Set<Node> nodes,
            boolean initialLoad) {
        TriggerRouter trigger = dataMetaData.getTriggerRouter();
        String subSelect = trigger.getRouter().getRouterExpression();
        Set<String> nodeIds = null;
        if (!StringUtils.isBlank(subSelect)) {
            SimpleJdbcTemplate simpleTemplate = new SimpleJdbcTemplate(jdbcTemplate);
            Map<String, Object> sqlParams = getDataObjectMap(dataMetaData, dbDialect);
            sqlParams.put("NODE_GROUP_ID", trigger.getRouter().getNodeGroupLink().getTargetNodeGroupId());
            List<String> ids = simpleTemplate.query(String.format("%s%s", sql, subSelect),
                    new SingleColumnRowMapper<String>(), sqlParams);
            if (ids != null) {
                nodeIds = new HashSet<String>(ids);
            }
        } else {
            nodeIds = toNodeIds(nodes, null);
        }
        return nodeIds;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public void setJdbcTemplate(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public void setDbDialect(IDbDialect dbDialect) {
        this.dbDialect = dbDialect;
    }

}