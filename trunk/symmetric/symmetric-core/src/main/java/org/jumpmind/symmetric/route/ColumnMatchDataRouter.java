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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.symmetric.common.TokenConstants;
import org.jumpmind.symmetric.model.DataMetaData;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.Router;
import org.jumpmind.symmetric.service.IConfigurationService;

/**
 * This data router is invoked when the router_type='column'. The
 * router_expression is always a name value pair of a column on the table that
 * is being synchronized to the value it should be matched with.
 * <P>
 * The value can be a constant. In the data router the value of the new data is
 * always represented by a string so all comparisons are done in the format that
 * SymmetricDS transmits.
 * <P>
 * The column name used for the match is the upper case column name if the
 * current value is being compared. The upper case column name prefixed by OLD_
 * can be used if the comparison is being done of the old data.
 * <P>
 * For example, if the column on a table is named STATUS you can specify that
 * you want to router when STATUS=OK by specifying such for the
 * router_expression. If you wanted to route when only the old value for
 * STATUS=OK you would specify OLD_STATUS=OK.
 * <P>
 * The value can also be one of the following expressions:
 * <ol>
 * <li>:NODE_ID</li>
 * <li>:EXTERNAL_ID</li>
 * <li>:NODE_GROUP_ID</li>
 * <li>:REDIRECT_NODE</li>
 * <li>:{column name}</li>
 * </ol>
 * NODE_ID, EXTERNAL_ID, and NODE_GROUP_ID are instructions for the column
 * matcher to select nodes that have a NODE_ID, EXTERNAL_ID or NODE_GROUP_ID
 * that are equal to the value on the column.
 * <P>
 * REDIRECT_NODE is an instruction to match the specified column to a
 * registrant_external_id on registration_redirect and return the associated
 * registration_node_id in the list of node id to route to. For example, if the
 * 'price' table was being routed to to a region 1 node based on the store_id,
 * the store_id would be the external_id of a node in the registration_redirect
 * table and the router_expression for trigger entry for the 'price' table would
 * be 'store_id=:REDIRECT_NODE' and the router_type would be 'column'.
 */
public class ColumnMatchDataRouter extends AbstractDataRouter implements IDataRouter {

    private static final String NULL_VALUE = "NULL";

    private IConfigurationService configurationService;

    final static String EXPRESSION_KEY = String.format("%s.Expression.", ColumnMatchDataRouter.class
            .getName());        
    
    public ColumnMatchDataRouter() {
    }

    public ColumnMatchDataRouter(IConfigurationService configurationService) {
        this.configurationService = configurationService;
    }

    public Set<String> routeToNodes(SimpleRouterContext routingContext,
            DataMetaData dataMetaData, Set<Node> nodes, boolean initialLoad) {
        Set<String> nodeIds = null;
        List<Expression> expressions = getExpressions(dataMetaData.getTriggerRouter().getRouter(), routingContext);
        Map<String, String> columnValues = getDataMap(dataMetaData);

        if (columnValues != null) {
            for (Expression e : expressions) {
                String column = e.tokens[0].trim();
                String value = e.tokens[1];
                if (value.equalsIgnoreCase(TokenConstants.NODE_ID)) {
                    for (Node node : nodes) {
                        if (e.equals && node.getNodeId().equals(columnValues.get(column))) {
                            nodeIds = addNodeId(node.getNodeId(), nodeIds, nodes);
                        }
                    }
                } else if (value.equalsIgnoreCase(TokenConstants.EXTERNAL_ID)) {
                    for (Node node : nodes) {
                        if (e.equals && node.getExternalId().equals(columnValues.get(column))) {
                            nodeIds = addNodeId(node.getNodeId(), nodeIds, nodes);
                        }
                    }
                } else if (value.equalsIgnoreCase(TokenConstants.NODE_GROUP_ID)) {
                    for (Node node : nodes) {
                        if (e.equals && node.getNodeGroupId().equals(columnValues.get(column))) {
                            nodeIds = addNodeId(node.getNodeId(), nodeIds, nodes);
                        }
                    }
                } else if (e.equals && value.equalsIgnoreCase(TokenConstants.REDIRECT_NODE)) {
                    Map<String, String> redirectMap = getRedirectMap(routingContext);
                    String nodeId = redirectMap.get(columnValues.get(column));
                    if (nodeId != null) {
                        nodeIds = addNodeId(nodeId, nodeIds, nodes);
                    }
                } else if (value.startsWith(":")) {
                    String firstValue = columnValues.get(column);
                    String secondValue = columnValues.get(value.substring(1));
                    if (e.equals
                            && ((firstValue == null && secondValue == null) || (firstValue != null
                                    && secondValue != null && firstValue.equals(secondValue)))) {
                        nodeIds = toNodeIds(nodes, nodeIds);
                    } else if (!e.equals
                            && ((firstValue != null && secondValue == null)
                                    || (firstValue == null && secondValue != null) || (firstValue != null
                                    && secondValue != null && !firstValue.equals(secondValue)))) {
                        nodeIds = toNodeIds(nodes, nodeIds);
                    }
                } else {
                    if (e.equals && (value.equals(columnValues.get(column)) || 
                            (value.equals(NULL_VALUE) && columnValues.get(column) == null))) {
                        nodeIds = toNodeIds(nodes, nodeIds);
                    } else if (!e.equals && ((!value.equals(NULL_VALUE) && !value.equals(columnValues.get(column))) || 
                            (value.equals(NULL_VALUE) && columnValues.get(column) != null))) {
                        nodeIds = toNodeIds(nodes, nodeIds);
                    }
                }

            }
        } else {
            log.warn("There were no columns to match for the data_id of %d", dataMetaData.getData().getDataId());
        }

        return nodeIds;

    }

    /**
     * Cache parsed expressions in the context to minimize the amount of parsing
     * we have to do when we have lots of throughput.
     */
    @SuppressWarnings("unchecked")
    protected List<Expression> getExpressions(Router router, SimpleRouterContext context) {
        final String KEY = EXPRESSION_KEY + router.getRouterId();
        List<Expression> expressions = (List<Expression>) context.getContextCache().get(
                KEY);
        if (expressions == null) {
            expressions = parse(router.getRouterExpression());
            context.getContextCache().put(KEY, expressions);
        }
        return expressions;
    }
    
    protected List<Expression> parse(String routerExpression) {
        List<Expression> expressions = new ArrayList<Expression>();       
        if (!StringUtils.isBlank(routerExpression)) {           
            String[] expTokens = routerExpression.split("\r\n|\r|\n|\\s*or\\s*|\\s*OR\\s*");
            if (expTokens != null) {
                for (String t : expTokens) {
                    if (!StringUtils.isBlank(t)) {
                        String[] tokens = null;
                        boolean equals = !t.contains("!=");
                        if (!equals) {
                            tokens = t.split("!=");
                        } else {
                            tokens = t.split("=");
                        }
                        if (tokens.length == 2) {
                            expressions.add(new Expression(equals, tokens));
                        } else {
                            log.warn("The provided column match expression was invalid: %s.  The full expression is %s.", t, routerExpression);
                        }

                    }
                }
            }
        } else {
            log.warn("The provided column match expression was invalid: %s.  The full expression is %s.", routerExpression, routerExpression);
        }
        return expressions;
    }

    @SuppressWarnings("unchecked")
    protected Map<String, String> getRedirectMap(SimpleRouterContext ctx) {
        final String CTX_CACHE_KEY = ColumnMatchDataRouter.class.getSimpleName() + "RouterMap";
        Map<String, String> redirectMap = (Map<String, String>) ctx.getContextCache().get(
                CTX_CACHE_KEY);
        if (redirectMap == null) {
            redirectMap = configurationService.getRegistrationRedirectMap();
            ctx.getContextCache().put(CTX_CACHE_KEY, redirectMap);
        }
        return redirectMap;
    }

    class Expression {
        boolean equals;
        String[] tokens;

        public Expression(boolean equals, String[] tokens) {
            this.equals = equals;
            this.tokens = tokens;
        }
    }
}