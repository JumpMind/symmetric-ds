package org.jumpmind.util;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class KeyedCache<K, T> implements Serializable {

    private static final long serialVersionUID = 1L;

    long timeoutTimeInMs;

    long lastRefreshTimeMs = 0;

    protected LinkedHashMap<K, T> keyedCache = new LinkedHashMap<K, T>();

    protected IRefreshCache<K, T> refresher;

    public KeyedCache(long timeoutTimeInMs, IRefreshCache<K, T> refresher) {
        this.timeoutTimeInMs = timeoutTimeInMs;
        this.refresher = refresher;
    }
    
    public boolean containsKey(String key) {
        refreshCacheIfNeeded(false);
        return keyedCache.containsKey(key);
    }

    public T find(K key, boolean refreshCache) {
        refreshCacheIfNeeded(refreshCache);
        return keyedCache.get(key);
    }

    public List<T> getAll(boolean refreshCache) {
        refreshCacheIfNeeded(refreshCache);
        return new ArrayList<T>(keyedCache.values());
    }

    public void clear() {
        lastRefreshTimeMs = 0;
    }

    protected void refreshCacheIfNeeded(boolean refreshCache) {
        Map<K, T> copy = keyedCache;
        if (copy == null || refreshCache || (System.currentTimeMillis() - lastRefreshTimeMs) > timeoutTimeInMs) {
            synchronized (this) {
                if (copy == null || refreshCache || (System.currentTimeMillis() - lastRefreshTimeMs) > timeoutTimeInMs) {
                    refreshCache();
                }
            }
        }
    }
    
    protected void refreshCache() {
        keyedCache = refresher.refresh();
        lastRefreshTimeMs = System.currentTimeMillis();
    }

    public interface IRefreshCache<K, T> {
        public LinkedHashMap<K, T> refresh();
    }

}
