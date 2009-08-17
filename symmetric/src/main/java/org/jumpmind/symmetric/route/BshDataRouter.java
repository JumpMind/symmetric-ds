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
 * In Progress ... TODO javadoc and unit test
 */
public class BshDataRouter extends AbstractDataRouter {

    protected IDbDialect dbDialect;

    public Collection<String> routeToNodes(IRouterContext context, DataMetaData dataMetaData, Set<Node> nodes,
            boolean initialLoad) {
        try {
            long ts = System.currentTimeMillis();
            Interpreter interpreter = getInterpreter(context);
            context.incrementStat(System.currentTimeMillis() - ts, "bsh.init");
            HashSet<String> targetNodes = new HashSet<String>();
            ts = System.currentTimeMillis();
            bind(interpreter, dataMetaData, nodes, targetNodes);
            context.incrementStat(System.currentTimeMillis() - ts, "bsh.bind");
            ts = System.currentTimeMillis();
            Object returnValue = interpreter.eval(dataMetaData.getTrigger().getRouter().getRouterExpression());
            context.incrementStat(System.currentTimeMillis() - ts, "bsh.eval");
            return eval(returnValue, nodes, targetNodes);
        } catch (EvalError e) {
            log.error("Error in data router.  Routing to nobody.", e);
            return Collections.emptySet();
        }
    }

    protected Interpreter getInterpreter(IRouterContext context) {
        final String KEY = String.format("%s.Interpreter", getClass().getName());
        Interpreter interpreter = (Interpreter) context.getContextCache().get(KEY);
        if (interpreter == null) {
            interpreter = new Interpreter();
            context.getContextCache().put(KEY, interpreter);
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
            return toNodeIds(nodes);
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
