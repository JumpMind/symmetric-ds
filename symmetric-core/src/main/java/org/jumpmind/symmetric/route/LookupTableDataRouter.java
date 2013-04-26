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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.sql.ISqlRowMapper;
import org.jumpmind.db.sql.ISqlTemplate;
import org.jumpmind.db.sql.Row;
import org.jumpmind.symmetric.SyntaxParsingException;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.model.DataMetaData;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.Router;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A data router that uses a lookup table to map data to nodes
 */
public class LookupTableDataRouter extends AbstractDataRouter implements IDataRouter {

    final static Logger log = LoggerFactory.getLogger(LookupTableDataRouter.class);

    public final static String PARAM_TABLE = "LOOKUP_TABLE";
    public final static String PARAM_KEY_COLUMN = "KEY_COLUMN";
    public final static String PARAM_MAPPED_KEY_COLUMN = "LOOKUP_KEY_COLUMN";
    public final static String PARAM_EXTERNAL_ID_COLUMN = "EXTERNAL_ID_COLUMN";

    final static String EXPRESSION_KEY = String.format("%s.Expression.",
            LookupTableDataRouter.class.getName());

    final static String LOOKUP_TABLE_KEY = String.format("%s.Table.",
            LookupTableDataRouter.class.getName());

    private ISymmetricDialect symmetricDialect;

    public LookupTableDataRouter(ISymmetricDialect symmetricDialect) {
        this.symmetricDialect = symmetricDialect;
    }

    public LookupTableDataRouter() {
    }

    public Set<String> routeToNodes(SimpleRouterContext routingContext, DataMetaData dataMetaData,
            Set<Node> nodes, boolean initialLoad) {

        Set<String> nodeIds = null;
        Router router = dataMetaData.getTriggerRouter().getRouter();
        Map<String, String> params = null;
        
        params = getParams(router, routingContext);
    
        Map<String, String> dataMap = getDataMap(dataMetaData, symmetricDialect);
        Map<String, Set<String>> lookupTable = getLookupTable(params, router, routingContext);
        String column = params.get(PARAM_KEY_COLUMN);
        String keyData = dataMap.get(column);
        Set<String> externalIds = lookupTable.get(keyData);
        if (externalIds != null) {
            for (Node node : nodes) {
                if (externalIds.contains(node.getExternalId())) {
                    nodeIds = addNodeId(node.getNodeId(), nodeIds, nodes);
                }
            }
        }

        return nodeIds;
    }

    /**
     * Cache parsed expressions in the context to minimize the amount of parsing
     * we have to do when we have lots of throughput.
     */
    @SuppressWarnings("unchecked")
    protected Map<String, String> getParams(Router router, SimpleRouterContext routingContext) {
        final String KEY = EXPRESSION_KEY + router.getRouterId();
        Map<String, String> params = (Map<String, String>) routingContext.getContextCache()
                .get(KEY);
        if (params == null) {
            params = parse(router.getRouterExpression());
            routingContext.getContextCache().put(KEY, params);
        }
        return params;
    }
    
    public Map<String, String> parse(String routerExpression) throws SyntaxParsingException {
        boolean valid = true;
        Map<String, String> params = new HashMap<String, String>();
        if (!StringUtils.isBlank(routerExpression)) {
            String[] expTokens = routerExpression.split("\r\n|\r|\n");
            if (expTokens != null) {
                for (String t : expTokens) {
                    if (!StringUtils.isBlank(t)) {
                        String[] tokens = t.split("=");
                        /*
                         * tokens must be trimmed, removing leading and trailing spaces
                         */
                        if (tokens.length == 2 && !params.containsKey(tokens[0].trim())) {
                            params.put(tokens[0].trim(), tokens[1].trim());
                        } else {
                            valid = false;
                            break;
                        }
                    }
                }
                if (!valid ||
                    params.size() != 4 || 
                    !params.containsKey(PARAM_TABLE) ||
                    !params.containsKey(PARAM_KEY_COLUMN) ||
                    !params.containsKey(PARAM_MAPPED_KEY_COLUMN) ||
                    !params.containsKey(PARAM_EXTERNAL_ID_COLUMN)) {
                    
                    log.warn("The provided lookup table router expression was invalid. The full expression is " + routerExpression + ".");
                    throw new SyntaxParsingException("The provided lookup table router expression was invalid. The full expression is " + routerExpression + ".");
                }
            }
        }
        else {
            log.warn("The provided lookup table router expression is empty");
        }
        return params;
    }    
    @SuppressWarnings("unchecked")
    protected Map<String, Set<String>> getLookupTable(final Map<String, String> params, Router router,
            SimpleRouterContext routingContext) {
        final String CTX_CACHE_KEY = LOOKUP_TABLE_KEY + "." + params.get("TABLENAME");
        Map<String, Set<String>> lookupMap = (Map<String, Set<String>>) routingContext
                .getContextCache().get(CTX_CACHE_KEY);
        if (lookupMap == null) {
            ISqlTemplate template = symmetricDialect.getPlatform().getSqlTemplate();
            final Map<String, Set<String>> fillMap = new HashMap<String, Set<String>>();
            template.query(String.format("select %s, %s from %s",
                    params.get(PARAM_MAPPED_KEY_COLUMN), params.get(PARAM_EXTERNAL_ID_COLUMN),
                    params.get(PARAM_TABLE)), new ISqlRowMapper<Object>() {
                public Object mapRow(Row rs) {
                    String key = rs.getString(params.get(PARAM_MAPPED_KEY_COLUMN));
                    String value = rs.getString(params.get(PARAM_EXTERNAL_ID_COLUMN));
                    Set<String> ids = fillMap.get(key);
                    if (ids == null) {
                        ids = new HashSet<String>();
                        fillMap.put(key, ids);
                    }
                    ids.add(value);
                    return value;
                }
            });
            lookupMap = fillMap;
            routingContext.getContextCache().put(CTX_CACHE_KEY, lookupMap);
        }
        return lookupMap;
    }

}
