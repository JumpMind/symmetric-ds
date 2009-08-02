package org.jumpmind.symmetric.integrate;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;

import org.jumpmind.symmetric.model.DataMetaData;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.route.IDataRouter;
import org.jumpmind.symmetric.route.IRouterContext;

public class XmlPublisherDataRouter extends AbstractXmlPublisherExtensionPoint implements IDataRouter {

    public void completeBatch(IRouterContext context) {
        // TODO publish XML
    }

    public Collection<String> routeToNodes(IRouterContext context, DataMetaData dataMetaData, Set<Node> nodes,
            boolean initialLoad) {
        // TODO build XML using abstract methods.  Work at continuing to refactor the XmlPublisherDataRouter
        return Collections.emptySet();
    }


}
