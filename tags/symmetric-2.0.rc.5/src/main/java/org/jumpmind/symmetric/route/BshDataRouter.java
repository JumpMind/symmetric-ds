package org.jumpmind.symmetric.route;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.jumpmind.symmetric.db.IDbDialect;
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

    protected IDbDialect dbDialect;
    
    final static String INTERPRETER_KEY = String.format("%s.Interpreter", BshDataRouter.class.getName());

    public Collection<String> routeToNodes(IRouterContext context, DataMetaData dataMetaData, Set<Node> nodes,
            boolean initialLoad) {
        try {
            long ts = System.currentTimeMillis();
            Interpreter interpreter = getInterpreter(context);
            context.incrementStat(System.currentTimeMillis() - ts, "bsh.init.ms");
            HashSet<String> targetNodes = new HashSet<String>();
            ts = System.currentTimeMillis();
            bind(interpreter, dataMetaData, nodes, targetNodes);
            context.incrementStat(System.currentTimeMillis() - ts, "bsh.bind.ms");
            ts = System.currentTimeMillis();
            Object returnValue = interpreter.eval(dataMetaData.getTriggerRouter().getRouter().getRouterExpression());
            context.incrementStat(System.currentTimeMillis() - ts, "bsh.eval.ms");
            return eval(returnValue, nodes, targetNodes);
        } catch (EvalError e) {
            log.error("Error in data router.  Routing to nobody.", e);
            return Collections.emptySet();
        }
    }

    protected Interpreter getInterpreter(IRouterContext context) {
        Interpreter interpreter = (Interpreter) context.getContextCache().get(INTERPRETER_KEY);
        if (interpreter == null) {
            interpreter = new Interpreter();
            context.getContextCache().put(INTERPRETER_KEY, interpreter);
        }
        return interpreter;
    }

    protected Collection<String> eval(Object value, Set<Node> nodes, Set<String> targetNodes) {
        if (targetNodes.size() > 0) {
            return targetNodes;
        } else if (value instanceof Collection<?>) {
            Collection<?> values = (Collection<?>) value;
            Set<String> nodeIds = new HashSet<String>(values.size());
            for (Object v : values) {
                if (v != null) {
                    nodeIds.add(v.toString());
                }
            }
            return nodeIds;
        } else if (value instanceof Boolean && value.equals(Boolean.TRUE)) {
            return toNodeIds(nodes, null);
        } else {
            return Collections.emptySet();
        }
    }

    protected void bind(Interpreter interpreter, DataMetaData dataMetaData, Set<Node> nodes, Set<String> targetNodes)
            throws EvalError {
        interpreter.set("nodes", nodes);
        interpreter.set("targetNodes", targetNodes);
        Map<String, Object> params = getDataObjectMap(dataMetaData, dbDialect);
        if (params != null) {
            for (String param : params.keySet()) {
                interpreter.set(param, params.get(param));
            }
        }
    }

    public void setDbDialect(IDbDialect dbDialect) {
        this.dbDialect = dbDialect;
    }
}
