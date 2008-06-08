package org.jumpmind.symmetric.config;

public interface IParameterFilter {

    /**
     * @param key
     * @param value
     * @return the new value
     */
    public String filterParameter(String key, String value);
}
