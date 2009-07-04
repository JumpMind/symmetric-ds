package org.jumpmind.symmetric.route;


public abstract class AbstractDataRouter implements IDataRouter {

    private boolean autoRegister;

    public boolean isAutoRegister() {
        return autoRegister;
    }
    
    public void setAutoRegister(boolean autoRegister) {
        this.autoRegister = autoRegister;
    }

}
