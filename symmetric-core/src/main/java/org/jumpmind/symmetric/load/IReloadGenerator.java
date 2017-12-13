package org.jumpmind.symmetric.load;

import java.util.List;

import org.jumpmind.extension.IExtensionPoint;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.TriggerHistory;

public interface IReloadGenerator extends IExtensionPoint {
    
    List<TriggerHistory> getActiveTriggerHistories(Node targetNode);
}
