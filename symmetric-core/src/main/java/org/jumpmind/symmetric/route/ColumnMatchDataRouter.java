/**
 * Licensed to JumpMind Inc under one or more contributor
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU General Public License, version 3.0 (GPLv3)
 * (the "License"); you may not use this file except in compliance
 * with the License.
 *
 * You should have received a copy of the GNU General Public License,
 * version 3.0 (GPLv3) along with this library; if not, see
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.jumpmind.extension.IBuiltInExtensionPoint;
import org.jumpmind.symmetric.SyntaxParsingException;
import org.jumpmind.symmetric.common.TokenConstants;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.model.DataMetaData;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.Router;
import org.jumpmind.symmetric.model.TriggerRouter;
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
public class ColumnMatchDataRouter extends AbstractDataRouter implements IDataRouter, IBuiltInExtensionPoint {

    private static final String NULL_VALUE = "NULL";

    private IConfigurationService configurationService;
    
    private ISymmetricDialect symmetricDialect;

    final static String EXPRESSION_KEY = String.format("%s.Expression.", ColumnMatchDataRouter.class
            .getName());        
    
    public ColumnMatchDataRouter() {
    }

    public ColumnMatchDataRouter(IConfigurationService configurationService, ISymmetricDialect symmetricDialect) {
        this.configurationService = configurationService;
        this.symmetricDialect = symmetricDialect;
    }

    public Set<String> routeToNodes(SimpleRouterContext routingContext,
            DataMetaData dataMetaData, Set<Node> nodes, boolean initialLoad, boolean initialLoadSelectUsed, TriggerRouter triggerRouter) {
        Set<String> nodeIds = null;
        if (initialLoadSelectUsed && initialLoad) {
            nodeIds = toNodeIds(nodes, null);
        } else {
            List<Expression> expressions = getExpressions(dataMetaData.getRouter(), routingContext);
            Map<String, String> columnValues = getDataMap(dataMetaData, symmetricDialect);

            if (columnValues != null) {
                for (Expression e : expressions) {
                    String column = e.tokens[0].trim();
                    String value = e.tokens[1];
                    String columnValue = columnValues.get(column);

                    if (value.equalsIgnoreCase(TokenConstants.NODE_ID)) {
                        for (Node node : nodes) {
                            nodeIds = runExpression(e, columnValue, node.getNodeId(), nodes,
                                    nodeIds, node);
                        }
                    } else if (value.equalsIgnoreCase(TokenConstants.EXTERNAL_ID)) {
                        for (Node node : nodes) {
                            nodeIds = runExpression(e, columnValue, node.getExternalId(), nodes,
                                    nodeIds, node);
                        }
                    } else if (value.equalsIgnoreCase(TokenConstants.NODE_GROUP_ID)) {
                        for (Node node : nodes) {
                            nodeIds = runExpression(e, columnValue, node.getNodeGroupId(), nodes,
                                    nodeIds, node);
                        }
                    } else if (e.hasEquals && value.equalsIgnoreCase(TokenConstants.REDIRECT_NODE)) {
                        Map<String, String> redirectMap = getRedirectMap(routingContext);
                        String nodeId = redirectMap.get(columnValue);
                        if (nodeId != null) {
                            nodeIds = addNodeId(nodeId, nodeIds, nodes);
                        }
                    } else {
                        String compareValue = value;
                        if (value.equalsIgnoreCase(TokenConstants.EXTERNAL_DATA)) {
                            compareValue = dataMetaData.getData().getExternalData();
                        } else if (value.startsWith(":")) {
                            compareValue = columnValues.get(value.substring(1));
                        } else if (value.equals(NULL_VALUE)) {
                            compareValue = null;
                        }
                        nodeIds = runExpression(e, columnValue, compareValue, nodes, nodeIds, null);
                    }
                }
            } else {
                log.warn("There were no columns to match for the data_id of {}", dataMetaData
                        .getData().getDataId());
            }
        }
        
        if(nodeIds != null) {
            nodeIds.remove(null);
        } else {
            nodeIds = Collections.emptySet();
        }

        return nodeIds;

    }

    protected Set<String> runExpression(Expression e, String columnValue, String compareValue, Set<Node> nodes, Set<String> nodeIds, Node node) {
        boolean result = false;
        if (e.hasEquals && ((columnValue == null && compareValue == null) || 
                (columnValue != null && columnValue.equals(compareValue)))) {
            result = true;
        } else if (e.hasNotEquals && ((columnValue == null && compareValue != null) || 
                (columnValue != null && !columnValue.equals(compareValue)))) {
            result = true;
        } else if (e.hasContains && columnValue != null && compareValue != null && 
                ArrayUtils.contains(columnValue.split(","), compareValue)) {
            result = true;
        } else if (e.hasNotContains && columnValue != null && compareValue != null && 
                !ArrayUtils.contains(columnValue.split(","), compareValue)) {
            result = true;
        }
        if (result) {
            if (node != null) {
                nodeIds = addNodeId(node.getNodeId(), nodeIds, nodes);
            } else {
                nodeIds = toNodeIds(nodes, nodeIds);
            }
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
    
    public List<Expression> parse(String routerExpression) throws SyntaxParsingException {
        List<Expression> expressions = new ArrayList<Expression>();       
        if (!StringUtils.isBlank(routerExpression)) {           
            
            String[] operators = { Expression.NOT_EQUALS, Expression.EQUALS, Expression.NOT_CONTAINS, Expression.CONTAINS};
            String[] expTokens = routerExpression.split("\\s*(\\s+or|\\s+OR)?(\r\n|\r|\n)(or\\s+|OR\\s+)?\\s*" +
            		                                    "|\\s+or\\s+" +
            		                                    "|\\s+OR\\s+");
            
            if (expTokens != null) {
                for (String t : expTokens) {
                    if (!StringUtils.isBlank(t)) {
                        boolean isFound = false;
                        for (String operator : operators) {
                            if (t.contains(operator)) {
                                String[] tokens = t.split(operator);
                                if (tokens.length == 2) {
                                    tokens[0] = parseColumn(tokens[0]);
                                    tokens[1] = parseValue(tokens[1]);
                                    expressions.add(new Expression(operator, tokens));
                                    isFound = true;
                                    break;
                                }
                            }
                        }
                            
                        if (!isFound) {
                            log.warn("The provided column match expression was invalid: {}.  The full expression is {}.", t, routerExpression);
                            throw new SyntaxParsingException("The provided column match expression was invalid: " + t + ".  The full expression is " + routerExpression + ".");
                        }

                    }
                }
            }
        } else {
            log.warn("The provided column match expression is empty");
        }
        return expressions;
    }

    /**
     * Parse a column (the first half of a column match expression).
     */
    private String parseColumn(String value) {
        return value.trim();
    }
    
    /**
     * Parse a value (the second half of a column match expression).
     */
    private String parseValue(String value) {
        value = value.trim();
        // Check for ticks around the value.
        if (value.charAt(0) == '\''
                && value.charAt(value.length() - 1) == '\'') {
            // remove first and last tick
            value = value.substring(1,value.length()-1);
            // replace all double ticks with a single tick only if value was surrounded with ticks
            value = value.replaceAll("''", "'");
        }
        return value;
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

    public static class Expression {
        public static final String EQUALS = "=";
        public static final String NOT_EQUALS = "!=";
        public static final String CONTAINS = "contains";
        public static final String NOT_CONTAINS = "not contains";
        
        boolean hasEquals;
        boolean hasNotEquals;
        boolean hasContains;
        boolean hasNotContains;
        String[] tokens;
        String operator;

        public Expression(String operator, String[] tokens) {
            this.tokens = tokens;
            this.operator = operator;
            if (operator.equals(EQUALS)) hasEquals = true;
            else if (operator.equals(NOT_EQUALS)) hasNotEquals = true;
            else if (operator.equals(CONTAINS)) hasContains = true;
            else if (operator.equals(NOT_CONTAINS)) hasNotContains = true;
        }
        
        public String[] getTokens() {
            return tokens;
        }

        public String getOperator() {
            return operator;
        }

        public boolean hasEquals() {
            return hasEquals;
        }

        public boolean hasNotEquals() {
            return hasEquals;
        }

        public boolean hasContains() {
            return hasEquals;
        }

        public boolean hasNotContains() {
            return hasEquals;
        }
    }
}
