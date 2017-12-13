package org.jumpmind.symmetric.load;

import java.util.List;

import org.jumpmind.extension.IBuiltInExtensionPoint;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.TriggerHistory;

public class DefaultReloadGenerator implements IReloadGenerator, IBuiltInExtensionPoint {

    public static final String NAME = "default reload generator";

    ISymmetricEngine engine;
    
    public DefaultReloadGenerator(ISymmetricEngine engine) {
        this.engine = engine;
    }
    
    @Override
    public List<TriggerHistory> getActiveTriggerHistories(Node targetNode) {
        return engine.getTriggerRouterService().getActiveTriggerHistories();
    }

}
