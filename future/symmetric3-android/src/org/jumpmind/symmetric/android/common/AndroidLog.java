package org.jumpmind.symmetric.android.common;

import org.jumpmind.symmetric.core.common.Log;
import org.jumpmind.symmetric.core.common.LogLevel;
import org.jumpmind.symmetric.core.common.StringUtils;

public class AndroidLog extends Log {

    @Override
    public void log(LogLevel level, Throwable error, String msg, Object... params) {

        if (StringUtils.isNotBlank(msg) && params != null && params.length > 0) {
            msg = String.format(msg, params);
        }

        switch (level) {
        case DEBUG:
            android.util.Log.d(clazz.getSimpleName(), msg);
            break;
        case INFO:
            if (error == null) {
                android.util.Log.i(clazz.getSimpleName(), msg);
            } else {
                android.util.Log.i(clazz.getSimpleName(), msg, error);
            }
            break;
        case WARN:
            if (error == null) {
                android.util.Log.w(clazz.getSimpleName(), msg);
            } else {
                android.util.Log.w(clazz.getSimpleName(), msg, error);
            }
            break;
        case ERROR:
            if (error == null) {
                android.util.Log.e(clazz.getSimpleName(), msg);
            } else {
                android.util.Log.e(clazz.getSimpleName(), msg, error);
            }
            break;
        default:
            if (error == null) {
                android.util.Log.wtf(clazz.getSimpleName(), msg);
            } else {
                android.util.Log.wtf(clazz.getSimpleName(), msg, error);
            }
            break;
        }

    }

    @Override
    public boolean isDebugEnabled() {
        return android.util.Log.isLoggable(clazz.getSimpleName(), android.util.Log.DEBUG);
    }

}
