package org.jumpmind.symmetric.common.logging;

import org.apache.commons.logging.LogConfigurationException;

public class LogFactory {

    @SuppressWarnings("unchecked")
    public static Log getLog(Class clazz) throws LogConfigurationException {
        return new Log(org.apache.commons.logging.LogFactory.getLog(clazz));
    }
}
