package org.jumpmind.symmetric.config;

import org.jumpmind.extension.IExtensionPoint;

public interface IParameterFilter extends IExtensionPoint {

    /**
     * @param key
     * @param value
     * @return the new value
     */
    public String filterParameter(String key, String value);
}