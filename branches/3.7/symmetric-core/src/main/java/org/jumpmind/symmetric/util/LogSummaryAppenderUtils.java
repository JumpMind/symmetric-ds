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
