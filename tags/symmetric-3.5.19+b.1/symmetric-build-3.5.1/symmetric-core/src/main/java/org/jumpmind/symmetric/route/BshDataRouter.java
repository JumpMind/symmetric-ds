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

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.model.DataMetaData;
import org.jumpmind.symmetric.model.Node;

import bsh.EvalError;
import bsh.Interpreter;

/**
 * This data router is invoked when the router_type is 'bsh'. The router_expression is always a bean shell expression.  See
 * <a href='http://www.beanshell.org'>the bean shell site</a> for information about the capabilities of the bean shell scripting
 * language.
 * <P/>
 * Bound to the interpreter are the names of both the current and old column values.  They can be used in the expression.  They should 
 * always be referenced using upper case.  Also bound to the interpreter is a {@link Collection} of targetNodes.  The script is expected
 * to add the the list of target nodes a list of the node_ids that should be routed to.
 */
public class BshDataRouter extends AbstractDataRouter {

    protected ISymmetricEngine engine;
    
    final String INTERPRETER_KEY = String.format("%d.BshInterpreter", hashCode());
       
    public BshDataRouter(ISymmetricEngine engine) {
        this.engine = engine;
    }

    public Set<String> routeToNodes(SimpleRouterContext context, DataMetaData dataMetaData, Set<Node> nodes,
            boolean initialLoad) {
        try {
            long ts = System.currentTimeMillis();
            Interpreter interpreter = getInterpreter(context);
            context.incrementStat(System.currentTimeMillis() - ts, "bsh.init.ms");
            HashSet<String> targetNodes = new HashSet<String>();
            ts = System.currentTimeMillis();
            bind(interpreter, dataMetaData, nodes, targetNodes, initialLoad);
            context.incrementStat(System.currentTimeMillis() - ts, "bsh.bind.ms");
            ts = System.currentTimeMillis();
            Object returnValue = interpreter.eval(dataMetaData.getRouter().getRouterExpression());
            context.incrementStat(System.currentTimeMillis() - ts, "bsh.eval.ms");
            return eval(returnValue, nodes, targetNodes);
        } catch (EvalError e) {
            log.error("Error in data router: " + dataMetaData.getRouter() + ".  Routing to nobody.", e);
            return Collections.emptySet();
        }
    }    

    protected Interpreter getInterpreter(SimpleRouterContext context) {
        Interpreter interpreter = (Interpreter) context.getContextCache().get(INTERPRETER_KEY);
        if (interpreter == null) {
            interpreter = new Interpreter();
            context.getContextCache().put(INTERPRETER_KEY, interpreter);
        }
        return interpreter;
    }

    protected Set<String> eval(Object value, Set<Node> nodes, Set<String> targetNodes) {
        targetNodes.remove(null);
        if (targetNodes.size() > 0) {
            return targetNodes;
        } else if (value instanceof Set<?>) {
            Set<?> values = (Set<?>) value;
            Set<String> nodeIds = new HashSet<String>(values.size());
            for (Object v : values) {
                if (v != null) {
                    nodeIds.add(v.toString());
                }
            }
            return nodeIds;
        } else if (value instanceof Boolean && value.equals(Boolean.TRUE)) {
            return toNodeIds(nodes, null);
        } else if (value instanceof String) {
            Set<String> node = new HashSet<String>(1);
            node.add(value.toString());
            return node;
        } else {
            return Collections.emptySet();
        }
    }

    protected void bind(Interpreter interpreter, DataMetaData dataMetaData, Set<Node> nodes, Set<String> targetNodes, boolean initialLoad)
            throws EvalError {
        interpreter.set("log", log);
        interpreter.set("initialLoad", initialLoad);        
        interpreter.set("dataMetaData", dataMetaData);
        interpreter.set("nodes", nodes);
        interpreter.set("identityNodeId", engine.getNodeService().findIdentityNodeId());
        interpreter.set("targetNodes", targetNodes);
        interpreter.set("engine", engine);
        Map<String, Object> params = getDataObjectMap(dataMetaData, engine.getSymmetricDialect(), true);
        if (params != null) {
            for (String param : params.keySet()) {
                interpreter.set(param, params.get(param));
            }
        }
    }
}
