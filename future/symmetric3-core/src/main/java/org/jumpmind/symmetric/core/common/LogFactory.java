package org.jumpmind.symmetric.core.common;

import java.util.HashMap;
import java.util.Map;

public class LogFactory {
    
    private static Class<?> logClass;

    private static Map<Class<?>, Log> logs = new HashMap<Class<?>, Log>();
    
    public static void setLogClass(Class<?> logClass) {
        LogFactory.logClass = logClass;
        logs.clear();
    }

    static void checkInitialization() {
        if (logClass == null) {
            String clazzName = System.getProperty(Log.class.getName(), ConsoleLog.class.getName());
            try {
                logClass = Class.forName(clazzName);
                Object log = logClass.newInstance();
                if (!(log instanceof Log)) {
                    throw new ClassCastException(log.getClass().getName()
                            + " was not an instance of " + Log.class.getName());
                }
            } catch (Exception e) {
                e.printStackTrace();
                logClass = Log4jLog.class;
            } 
        }
    }

    public static Log getLog(Class<?> clazz) {
        Log log = logs.get(clazz);
        if (log == null) {
            synchronized (logs) {
                log = logs.get(clazz);
                if (log == null) {
                    checkInitialization();
                    try {
                        log = (Log) logClass.newInstance();
                    } catch (Exception e) {
                        log = new Log4jLog();
                    }

                    log.initialize(clazz);
                    logs.put(clazz, log);
                }
            }
        }
        return log;
    }
}
