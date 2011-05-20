package org.jumpmind.symmetric.core.common;

import java.util.HashMap;
import java.util.Map;

public class LogFactory {

    private static Class<?> logClass;

    private static Map<Class<?>, Log> logs = new HashMap<Class<?>, Log>();

    static {
        String clazzName = System.getProperty(Log.class.getName(), DefaultLog.class.getName());
        try {
            logClass = Class.forName(clazzName);
            Object log = logClass.newInstance();
            if (!(log instanceof Log)) {
                throw new ClassCastException(log.getClass().getName() + " was not an instance of "
                        + Log.class.getName());
            }
        } catch (Exception e) {
            e.printStackTrace();
            logClass = DefaultLog.class;
        }
    }

    public static Log getLog(Class<?> clazz) {
        Log log = logs.get(clazz);
        if (log == null) {
            synchronized (logs) {
                log = logs.get(clazz);
                if (log == null) {
                    try {
                        log = (Log) logClass.newInstance();
                    } catch (Exception e) {
                        log = new DefaultLog();
                    }

                    log.initialize(clazz);
                    logs.put(clazz, log);
                }
            }
        }
        return log;
    }
}
