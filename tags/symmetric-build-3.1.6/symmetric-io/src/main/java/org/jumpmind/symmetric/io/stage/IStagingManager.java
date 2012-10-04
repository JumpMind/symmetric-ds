package org.jumpmind.symmetric.io.stage;

public interface IStagingManager {

    public abstract IStagedResource find(Object... path);

    public abstract IStagedResource create(Object... path);

    public abstract long clean();

}
