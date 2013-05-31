package org.jumpmind.symmetric.io.stage;

public interface IStagingManager {

    public IStagedResource find(Object... path);

    public IStagedResource create(Object... path);

    public long clean();
    
    public long clean(long timeToLiveInMs);

}
