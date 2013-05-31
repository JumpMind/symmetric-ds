package org.jumpmind.symmetric.route;

import org.jumpmind.symmetric.model.Data;

public interface IDataToRouteReader extends Runnable {

    public abstract Data take() throws InterruptedException;

    public abstract boolean isReading();

    public abstract void setReading(boolean reading);

}