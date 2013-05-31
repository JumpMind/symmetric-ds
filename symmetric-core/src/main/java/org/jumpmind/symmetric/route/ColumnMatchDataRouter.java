package org.jumpmind.symmetric.route;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.symmetric.SyntaxParsingException;
import org.jumpmind.symmetric.common.TokenConstants;
import org.jumpmind.symmetric.db.ISymmetricDialect;
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
            DataMetaData dataMetaData, Set<Node> nodes, boolean initialLoad) {
        Set<String> nodeIds = null;
        List<Expression> expressions = getExpressions(dataMetaData.getRouter(), routingContext);
        Map<String, String> columnValues = getDataMap(dataMetaData, symmetricDialect);

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
            log.warn("There were no columns to match for the data_id of {}", dataMetaData.getData().getDataId());
        }
        
        if(nodeIds != null) {
            nodeIds.remove(null);
        } else {
            nodeIds = Collections.emptySet();
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
            
            String[] expTokens = routerExpression.split("\\s*(\\s+or|\\s+OR)?(\r\n|\r|\n)(or\\s+|OR\\s+)?\\s*" +
            		                                    "|\\s+or\\s+" +
            		                                    "|\\s+OR\\s+");
            
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
                            tokens[0] = parseColumn(tokens[0]);
                            tokens[1] = parseValue(tokens[1]);
                            expressions.add(new Expression(equals, tokens));
                        } else {
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

    public class Expression {
        boolean equals;
        String[] tokens;

        public Expression(boolean equals, String[] tokens) {
            this.equals = equals;
            this.tokens = tokens;
        }
        
        public String[] getTokens() {
            return tokens;
        }
        
        public boolean getEquals() {
            return equals;
        }
    }
}
