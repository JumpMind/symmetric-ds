package org.jumpmind.symmetric.common.logging;

import org.apache.commons.logging.LogConfigurationException;

public class LogFactory {

    @SuppressWarnings("unchecked")
    public static ILog getLog(Class clazz) throws LogConfigurationException {
        return new CommonsResourceLog(org.apache.commons.logging.LogFactory.getLog(clazz));
    }
}
