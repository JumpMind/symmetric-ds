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
package org.jumpmind.util;

import static org.apache.commons.lang3.StringUtils.isNotBlank;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;

public class LogSummaryAppender extends AbstractAppender {
    protected Map<String, Map<String, LogSummary>> errorsByEngineByMessage = new ConcurrentHashMap<String, Map<String, LogSummary>>();
    protected Map<String, Map<String, LogSummary>> warningByEngineByMessage = new ConcurrentHashMap<String, Map<String, LogSummary>>();
    protected Log4j2Helper helper = new Log4j2Helper();

    public LogSummaryAppender(String name, Filter filter) {
        super(name, filter, null, false, null);
    }

    @Override
    public void append(LogEvent event) {
        Map<String, Map<String, LogSummary>> summaries = null;
        if (event.getLevel() == Level.ERROR) {
            summaries = errorsByEngineByMessage;
        } else if (event.getLevel() == Level.WARN) {
            summaries = warningByEngineByMessage;
        }
        if (summaries != null) {
            String engineName = (String) event.getContextData().getValue("engineName");
            if (isNotBlank(engineName)) {
                Map<String, LogSummary> byMessage = summaries.get(engineName);
                if (byMessage == null) {
                    byMessage = new ConcurrentHashMap<String, LogSummary>();
                    summaries.put(engineName, byMessage);
                }
                String message = null;
                if (event.getMessage() != null && !event.getMessage().toString().equals("") &&
                        !event.getMessage().toString().equals("null")) {
                    message = event.getMessage().toString();
                } else {
                    if (event.getThrown() != null) {
                        Throwable t = event.getThrown();
                        message = t.getClass().getName() + ": " + t.getMessage();
                    } else {
                        message = "Unhandled error";
                    }
                }
                LogSummary summary = byMessage.get(message);
                if (summary == null) {
                    summary = new LogSummary();
                    summary.setMessage(message);
                    summary.setFirstOccurranceTime(event.getInstant().getEpochMillisecond());
                    byMessage.put(message, summary);
                }
                summary.setLevel(helper.convertFromLevel(event.getLevel()));
                summary.setMostRecentTime(event.getInstant().getEpochMillisecond());
                summary.setCount(summary.getCount() + 1);
                Throwable throwable = event.getThrown();
                summary.setThrowable(throwable);
                if (throwable != null) {
                    StringWriter st = new StringWriter();
                    throwable.printStackTrace(new PrintWriter(st));
                    summary.setStackTrace(st.toString());
                }
                summary.setMostRecentThreadName(event.getThreadName());
            }
        }
    }

    public List<LogSummary> getLogSummaries(String engineName, Level level) {
        Map<String, Map<String, LogSummary>> summaries = null;
        if (level == Level.ERROR) {
            summaries = errorsByEngineByMessage;
        } else if (level == Level.WARN) {
            summaries = warningByEngineByMessage;
        }
        List<LogSummary> list = new ArrayList<LogSummary>();
        if (summaries != null && summaries.get(engineName) != null) {
            list.addAll(summaries.get(engineName).values());
            Collections.sort(list, new Comparator<LogSummary>() {
                @Override
                public int compare(LogSummary o1, LogSummary o2) {
                    return Long.valueOf(o1.getMostRecentTime()).compareTo(o2.getMostRecentTime());
                }
            });
        }
        return list;
    }

    public void clearAll(String engineName) {
        errorsByEngineByMessage.remove(engineName);
        warningByEngineByMessage.remove(engineName);
    }

    public void purgeOlderThan(long time) {
        purgeOlderThan(time, errorsByEngineByMessage);
        purgeOlderThan(time, warningByEngineByMessage);
    }

    protected void purgeOlderThan(long time,
            Map<String, Map<String, LogSummary>> logSummaryByEngineByMessage) {
        Collection<Map<String, LogSummary>> all = logSummaryByEngineByMessage.values();
        for (Map<String, LogSummary> map : all) {
            Set<String> keys = map.keySet();
            for (String key : keys) {
                LogSummary summary = map.get(key);
                if (summary != null) {
                    if (summary.getMostRecentTime() < time) {
                        map.remove(key);
                    }
                }
            }
        }
    }
}
