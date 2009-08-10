package org.jumpmind.symmetric.route;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.jumpmind.symmetric.model.DataMetaData;
import org.jumpmind.symmetric.model.Node;

import bsh.EvalError;
import bsh.Interpreter;

/**
 * In Progress ...
 * TODO javadoc and unit test
 */
public class BshDataRouter extends AbstractDataRouter {

    public void completeBatch(IRouterContext context) {
    }

    public Collection<String> routeToNodes(IRouterContext context, DataMetaData dataMetaData, Set<Node> nodes,
            boolean initialLoad) {
        try {
            Interpreter interpreter = new Interpreter();
            interpreter.set("nodes", nodes);
            // set old and new and cur column values
            Object value = interpreter.eval(dataMetaData.getTrigger().getRouterExpression());
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
            }
        } catch (EvalError e) {
            logger.error(e, e);
        }
        return Collections.emptySet();
    }

}
