package org.jumpmind.symmetric.route;


public abstract class AbstractDataRouter implements IDataRouter {

    private boolean autoRegister;
    
    private boolean applyToInitialLoad = true;

    public boolean isAutoRegister() {
        return autoRegister;
    }
    
    public void setAutoRegister(boolean autoRegister) {
        this.autoRegister = autoRegister;
    }

    public void setApplyToInitialLoad(boolean applyToInitialLoad) {
        this.applyToInitialLoad = applyToInitialLoad;
    }
    
    public boolean isApplyToInitialLoad() {
        return applyToInitialLoad;
    }
}
