package org.jumpmind.symmetric.load;

import java.util.List;

import org.jumpmind.extension.IExtensionPoint;
import org.jumpmind.symmetric.ext.ISymmetricEngineAware;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.TriggerHistory;

public interface IReloadGenerator extends IExtensionPoint, ISymmetricEngineAware {
    
    List<TriggerHistory> getActiveTriggerHistories(Node targetNode);
}
