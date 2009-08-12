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
            Interpreter interpreter = new Interpreter();
            bind(interpreter, dataMetaData, nodes);
            Object value = interpreter.eval(dataMetaData.getTrigger().getRouterExpression());
            return eval(value, nodes);
        } catch (EvalError e) {
            logger.error("Error in data router.  Routing to nobody.", e);
            return Collections.emptySet();
        }
    }

    protected Collection<String> eval(Object value, Set<Node> nodes) {
        if (value instanceof Boolean && value.equals(Boolean.TRUE)) {
            return toNodeIds(nodes);
        } else if (value instanceof Collection<?>) {
            Collection<?> values = (Collection<?>) value;
            Set<String> nodeIds = new HashSet<String>(values.size());
            for (Object v : values) {
                if (v != null) {
                    nodeIds.add(v.toString());
                }
            }
            return nodeIds;
        } else {
            return Collections.emptySet();
        }
    }

    protected void bind(Interpreter interpreter, DataMetaData dataMetaData, Set<Node> nodes) throws EvalError {
        interpreter.set("nodes", nodes);
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
