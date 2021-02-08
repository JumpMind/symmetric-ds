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

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Filter.Result;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.ConsoleAppender;
import org.apache.logging.log4j.core.appender.FileAppender;
import org.apache.logging.log4j.core.appender.RollingFileAppender;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.filter.ThresholdFilter;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.apache.logging.log4j.core.config.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Compiles against log4j classes, so only instantiate this class if log4j is present
 */
public class Log4j2Helper {

    private static final Logger log = LoggerFactory.getLogger(Log4j2Helper.class);

    public void initialize(boolean isDebug) throws MalformedURLException {
        URL log4jUrl = new URL(System.getProperty("log4j2.configurationFile", "file:" + AppUtils.getSymHome() + "/conf/log4j2-blank.xml"));
        File log4jFile = new File(new File(log4jUrl.getFile()).getParent(), "log4j2.xml");

        if (isDebug) {
            log4jFile = new File(log4jFile.getParent(), "log4j2-debug.xml");
        }

        if (log4jFile.exists()) {
            Configurator.initialize("SYM", log4jFile.getAbsolutePath());
        }
    }

    public LogSummaryAppender registerLogSummaryAppenderInternal(String name) {
        LogSummaryAppender appender = new LogSummaryAppender(name,
                ThresholdFilter.createFilter(Level.WARN, Result.ACCEPT, Result.DENY));
        addAppender(appender);
        return appender;
    }

    public void registerConsoleAppender() {
        if (getAppender("CONSOLE") == null && getAppender("CONSOLE_ERR") == null) {
            PatternLayout layout = PatternLayout.newBuilder().withPattern("[%X{engineName}] - %c{1} - %m%ex%n").build();
            Appender appender = ConsoleAppender.newBuilder().setName("CONSOLE").setTarget(ConsoleAppender.Target.SYSTEM_ERR)
                    .setLayout(layout).build();
            addAppender(appender);
        }
    }

    public void registerVerboseConsoleAppender() {
        removeAppender("CONSOLE");
        PatternLayout layout = PatternLayout.newBuilder().withPattern("%d %-5p [%c{2}] [%t] %m%ex%n").build();
        Appender appender = ConsoleAppender.newBuilder().setName("CONSOLE").setTarget(ConsoleAppender.Target.SYSTEM_ERR)
                .setLayout(layout).build();
        addAppender(appender);
    }

    public void registerRollingFileAppender(String overrideLogFileName) {
        Appender appender = getAppender("ROLLING");
        if (appender instanceof SymRollingFileAppender) {
            SymRollingFileAppender fa = (SymRollingFileAppender) appender;
            String fileName = fa.getFileName();
            
            if (overrideLogFileName != null) {
                fileName = fileName.replace("symmetric.log", overrideLogFileName);
                LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
                Configuration config = ctx.getConfiguration();
                RollingFileAppender rolling = RollingFileAppender.newBuilder().setConfiguration(config).setName("ROLLING")
                    .withFileName(fileName)
                    .withFilePattern(fa.getFilePattern().replace("symmetric.log", overrideLogFileName))
                    .setLayout(fa.getLayout()).withPolicy(fa.getTriggeringPolicy())
                    .withStrategy(fa.getManager().getRolloverStrategy()).build();
                removeAppender("ROLLING");
                addAppender(rolling);
            }
            System.err.println(String.format("Log output will be written to %s", fileName));
        }
    }

    public Appender getAppender(String name) {
        LoggerContext lc = (LoggerContext) LogManager.getContext(false);
        return lc.getRootLogger().getAppenders().get(name);
    }

    public void addAppender(Appender appender) {
        try {
            LoggerContext lc = (LoggerContext) LogManager.getContext(false);
            appender.start();
            lc.getRootLogger().addAppender(appender);
            lc.updateLoggers();
        } catch (Exception ex) {
            // Can get ClassCastException log4j is not being used
            log.debug("Failed to add appender " + appender.getName(), ex);
        }
    }

    public void removeAppender(String name) {
        LoggerContext lc = (LoggerContext) LogManager.getContext(false);
        Appender appender = lc.getRootLogger().getAppenders().get(name);
        if (appender != null) {
            lc.getRootLogger().removeAppender(appender);
        }
    }

    public Level convertLevel(org.slf4j.event.Level level) {
        Level log4jLevel = Level.ERROR;
        return log4jLevel;
    }

    public File getLogDir() {
        Map<File, Layout<?>> fileLayouts = getLogFileLayout();
        if (fileLayouts != null && fileLayouts.size() > 0) {
            return fileLayouts.keySet().iterator().next().getParentFile();
        }
        return null;        
    }

    public File getLogFile() {
        Map<File, Layout<?>> fileLayouts = getLogFileLayout();
        if (fileLayouts != null && fileLayouts.size() > 0) {
            return fileLayouts.keySet().iterator().next();
        }
        return null;
    }
    
    public boolean isDefaultLogLayoutPattern() {
        Map<File, Layout<?>> fileLayouts = getLogFileLayout();
        if (fileLayouts != null && fileLayouts.size() > 0) {
            Layout<?> layout = fileLayouts.values().iterator().next();
            try {
                if (layout instanceof PatternLayout) {
                    return ((PatternLayout) layout).getConversionPattern().equals("%d %p [%X{engineName}] [%c{1}] [%t] %m%ex%n");
                }
            } catch (Exception e) {
                log.debug("Unable to determine log file layout pattern. ", e);
            }
        }
        return false;
    }
    
    public Map<File, Layout<?>> getLogFileLayout() {
        LoggerContext lc = (LoggerContext) LogManager.getContext(false);
        Map<String, Appender> appenderMap = lc.getRootLogger().getAppenders();
        for (Appender appender : appenderMap.values()) {
            String fileName = null;
            Layout<?> layout = null;
            if (appender instanceof FileAppender) {
                fileName = ((FileAppender) appender).getFileName();
                layout = ((FileAppender) appender).getLayout();
            } else if (appender instanceof RollingFileAppender) {
                fileName = ((RollingFileAppender) appender).getFileName();
                layout = ((RollingFileAppender) appender).getLayout();
            } else if (appender instanceof SymRollingFileAppender) {
                fileName = ((SymRollingFileAppender) appender).getFileName();
                layout = ((SymRollingFileAppender) appender).getLayout();
            }
            if (fileName != null) {
                File file = new File(fileName);
                if (file.exists()) {
                    Map<File, Layout<?>> matches = new HashMap<File, Layout<?>>();
                    matches.put(file, layout);
                    return matches;
                }
            }
        }
        return null;
    }

    public void setLevel(String loggerName, org.slf4j.event.Level level) {
        Configurator.setLevel(loggerName, convertToLevel(level));
    }

    public org.slf4j.event.Level getLevel(String loggerName) {
        return convertFromLevel(LogManager.getLogger(loggerName).getLevel());
    }
    
    public org.slf4j.event.Level getRootLevel() {
        return convertFromLevel(LogManager.getRootLogger().getLevel());
    }

    public org.slf4j.event.Level convertFromLevel(Level level) {
        org.slf4j.event.Level converted = null;
        if (level == Level.TRACE) {
            converted = org.slf4j.event.Level.TRACE;
        } else if (level == Level.DEBUG) {
            converted = org.slf4j.event.Level.DEBUG;
        } else if (level == Level.INFO) {
            converted = org.slf4j.event.Level.INFO;
        } else if (level == Level.WARN) {
            converted = org.slf4j.event.Level.WARN;
        } else if (level == Level.ERROR) {
            converted = org.slf4j.event.Level.ERROR;
        } else if (level == Level.FATAL) {
            converted = org.slf4j.event.Level.ERROR;
        } 
        return converted;
    }

    public Level convertToLevel(org.slf4j.event.Level level) {
        Level converted = null;
        if (level == org.slf4j.event.Level.TRACE) {
            converted = Level.TRACE;
        } else if (level == org.slf4j.event.Level.DEBUG) {
            converted = Level.DEBUG;
        } else if (level == org.slf4j.event.Level.INFO) {
            converted = Level.INFO;
        } else if (level == org.slf4j.event.Level.WARN) {
            converted = Level.WARN;
        } else if (level == org.slf4j.event.Level.ERROR) {
            converted = Level.ERROR;
        } 
        return converted;
    }

}
