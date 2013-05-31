package org.jumpmind.persist;

public interface IPersister<T, K> {

    public void save(T object, K key);
    
    public T get(Class<T> clazz, K key);
}
