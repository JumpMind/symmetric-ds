package org.jumpmind.symmetric.util;

import java.util.Collections;
import java.util.List;

import org.apache.log4j.Level;
import org.jumpmind.util.LogSummary;
import org.jumpmind.util.LogSummaryAppender;

public class LogSummaryAppenderUtils {

    private static final String LOG_SUMMARY_APPENDER_NAME = "SUMMARY";

    private LogSummaryAppenderUtils() {
    }

    public static void registerLogSummaryAppender() {
        LogSummaryAppender appender = getLogSummaryAppender();
        if (appender == null) {
            appender = new LogSummaryAppender();
            appender.setName(LOG_SUMMARY_APPENDER_NAME);
            appender.setThreshold(Level.WARN);
            org.apache.log4j.Logger.getRootLogger().addAppender(appender);
        }
    }

    public static LogSummaryAppender getLogSummaryAppender() {
        return (LogSummaryAppender) org.apache.log4j.Logger.getRootLogger().getAppender(
                LOG_SUMMARY_APPENDER_NAME);
    }

    public static void clearAllLogSummaries(String engineName) {
        LogSummaryAppender appender = getLogSummaryAppender();
        if (appender != null) {
            appender.clearAll(engineName);
        }
    }

    public static List<LogSummary> getLogSummaryWarnings(String engineName) {
        return getLogSummaries(engineName, Level.WARN);
    }

    public static List<LogSummary> getLogSummaryErrors(String engineName) {
        return getLogSummaries(engineName, Level.ERROR);
    }

    @SuppressWarnings("unchecked")
    public static List<LogSummary> getLogSummaries(String engineName, Level level) {
        LogSummaryAppender appender = getLogSummaryAppender();
        if (appender != null) {
            return appender.getLogSummaries(engineName, level);
        } else {
            return Collections.EMPTY_LIST;
        }
    }
}
