/**
 * Licensed to JumpMind Inc under one or more contributor
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU General Public License, version 3.0 (GPLv3)
 * (the "License"); you may not use this file except in compliance
 * with the License.
 *
 * You should have received a copy of the GNU General Public License,
 * version 3.0 (GPLv3) along with this library; if not, see
 * <http://www.gnu.org/licenses/>.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.jumpmind.symmetric.util;

import java.util.Collections;
import java.util.List;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Filter.Result;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.filter.ThresholdFilter;
import org.jumpmind.util.LogSummary;
import org.jumpmind.util.LogSummaryAppender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogSummaryAppenderUtils {

    private static final String LOG_SUMMARY_APPENDER_NAME = "SUMMARY";
    
    private static final Logger log = LoggerFactory.getLogger(LogSummaryAppenderUtils.class);

    private LogSummaryAppenderUtils() {
    }

    public static void registerLogSummaryAppender() {
        LogSummaryAppender appender = getLogSummaryAppender();
        if (appender == null) {
            registerLogSummaryAppenderInternal();
        }
    }
    
    public static LogSummaryAppender getLogSummaryAppender() {
        try {            
            LogSummaryAppender appender = (LogSummaryAppender) getAppender(LOG_SUMMARY_APPENDER_NAME);
            return appender;
        } catch (Exception ex) {
            // Can get ClassCastException if if the app has been recycled in the same container.
            log.debug("Failed to load appender " + LOG_SUMMARY_APPENDER_NAME, ex);
            try {                
                removeAppender(LOG_SUMMARY_APPENDER_NAME);
            } catch (Exception ex2) {
                log.debug("Failed to remove appender " + LOG_SUMMARY_APPENDER_NAME, ex2);    
            }
            return registerLogSummaryAppenderInternal();
        }
    }
    
    private static LogSummaryAppender registerLogSummaryAppenderInternal() {
        LogSummaryAppender appender = new LogSummaryAppender(LOG_SUMMARY_APPENDER_NAME,
                ThresholdFilter.createFilter(Level.WARN, Result.ACCEPT, Result.DENY));
        addAppender(appender);
        return appender;
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

    public static void addAppender(Appender appender) {
        try {
            LoggerContext lc = (LoggerContext) LogManager.getContext(false);
            appender.start();
            lc.getRootLogger().addAppender(appender);
            lc.updateLoggers();
        } catch (Exception ex) {
            // Can get ClassCastException log4j is not being used
            log.debug("Failed to add appender " + LOG_SUMMARY_APPENDER_NAME, ex);
        }
    }

    public static void removeAppender(String name) {
        LoggerContext lc = (LoggerContext) LogManager.getContext(false);
        Appender appender = lc.getRootLogger().getAppenders().get(name);
        if (appender != null) {
            lc.getRootLogger().removeAppender(appender);
        }
    }

    public static Appender getAppender(String name) {
        LoggerContext lc = (LoggerContext) LogManager.getContext(false);
        return lc.getRootLogger().getAppenders().get(name);
    }

}
