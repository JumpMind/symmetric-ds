package org.jumpmind.symmetric.transport;

/**
 * This marks a resource that is used by a transport. For instance for a
 * traditional application server, various HttpServlets might extends this. For
 * an alternative transport, a different technology would likely extend it.
 * 
 * 
 * @param <T>
 *
 * 
 */
public interface ITransportResource<T extends ITransportResourceHandler> {

    public abstract void setTransportResourceHandler(T transportResourceHandler);

    public abstract T getTransportResourceHandler();

}