package org.jumpmind.symmetric.load;

import org.jumpmind.db.sql.ISqlTransaction;
import org.jumpmind.extension.IExtensionPoint;
import org.jumpmind.symmetric.model.Node;

/**
 * This is an extension point that can be implemented to listen in and take
 * action before or after a reload is requested for a Node.
 */
public interface IReloadListener extends IExtensionPoint {

    public void beforeReload(ISqlTransaction transaction, Node node);

    public void afterReload(ISqlTransaction transaction , Node node);

}