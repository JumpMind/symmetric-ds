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

import static org.junit.Assert.*;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Category;
import org.apache.log4j.Level;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.Test;

public class SymRollingFileAppenderTest {

    @Test
    public void testDuplicatedLogMessages() throws Exception {
        ByteArrayOutputStream os = new ByteArrayOutputStream(1024);
        SymRollingFileAppender appender = getAppenderForTest(os);

        Exception ex = new Exception("Test exception.");

        LoggingEvent event1 = getLoggingEventForTest("Test Exception.", ex);
        LoggingEvent event2 = getLoggingEventForTest("Test Exception.", ex);
        LoggingEvent event3 = getLoggingEventForTest("Test Exception.", ex);

        appender.append(event1);
        appender.append(event2);
        appender.append(event3);

        String logging = os.toString("UTF8");

        // 2016-08-11 11:55:38,487 ERROR [] [SymRollingFileAppenderTest] [main] Test Exception. StackTraceKey.init [Exception:1478675418]
        Pattern initPattern = Pattern.compile(".*StackTraceKey.init \\[Exception:([0-9]*)\\].*", Pattern.DOTALL);
        Matcher m = initPattern.matcher(logging);
        if (m.matches()) {
            String stackTraceKey = "Exception:" + m.group(1);            
            assertEquals(3, StringUtils.countMatches(logging, stackTraceKey));
        } else {
            fail("Didn't find proper logging pattern.");
        }
    }

    @Test
    public void testDistinctLogMessages() throws Exception {
        ByteArrayOutputStream os = new ByteArrayOutputStream(1024);
        SymRollingFileAppender appender = getAppenderForTest(os);

        Exception ex = new Exception("Test exception.");
        Exception ex2 = new Exception("Test exception.");

        LoggingEvent event1 = getLoggingEventForTest("Test Exception.", ex);
        LoggingEvent event2 = getLoggingEventForTest("Test Exception.", ex2);

        appender.append(event1);
        appender.append(event2);

        String logging = os.toString("UTF8");

        // 2016-08-11 11:55:38,487 ERROR [] [SymRollingFileAppenderTest] [main] Test Exception. StackTraceKey.init [Exception:1478675418]
        Pattern initPattern = Pattern.compile(".*StackTraceKey.init \\[Exception:([0-9]*)\\].*", Pattern.DOTALL);
        Matcher m = initPattern.matcher(logging);
        if (m.matches()) {
            {                
                String stackTraceKey = "Exception:" + m.group(1);            
                assertEquals(1, StringUtils.countMatches(logging, stackTraceKey));
            }
            m.matches();
            {
                String stackTraceKey = "Exception:" + m.group(1);            
                assertEquals(1, StringUtils.countMatches(logging, stackTraceKey));
            }
        } else {
            fail("Didn't find proper logging pattern.");
        }
    }

    @Test
    public void testMixedMessages() throws Exception {
        ByteArrayOutputStream os = new ByteArrayOutputStream(1024);
        SymRollingFileAppender appender = getAppenderForTest(os);

        Exception ex = new Exception("Test exception.");
        Exception ex2 = new Exception("Test exception.");

        LoggingEvent event1 = getLoggingEventForTest("Test Exception.", ex);
        LoggingEvent event2 = getLoggingEventForTest("Test Exception.", ex2);
        LoggingEvent event3 = getLoggingEventForTest("Test Exception.", ex);
        LoggingEvent event4 = getLoggingEventForTest("Test Exception.", ex2);

        appender.append(event1);
        appender.append(event2);
        appender.append(event3);
        appender.append(event4);

        String logging = os.toString("UTF8");

        // 2016-08-11 11:55:38,487 ERROR [] [SymRollingFileAppenderTest] [main] Test Exception. StackTraceKey.init [Exception:1478675418]
        Pattern initPattern = Pattern.compile(".*StackTraceKey.init \\[Exception:([0-9]*)\\].*", Pattern.DOTALL);
        Matcher m = initPattern.matcher(logging);
        if (m.matches()) {
            {                
                String stackTraceKey = "Exception:" + m.group(1);            
                assertEquals(2, StringUtils.countMatches(logging, stackTraceKey));
            }
            m.matches();
            {
                String stackTraceKey = "Exception:" + m.group(1);            
                assertEquals(2, StringUtils.countMatches(logging, stackTraceKey));
            }
        } else {
            fail("Didn't find proper logging pattern.");
        }  
    }

    @Test
    public void testRollover() throws Exception {
        ByteArrayOutputStream os = new ByteArrayOutputStream(1024);
        SymRollingFileAppender appender = getAppenderForTest(os);

        Exception ex = new Exception("Test exception.");

        LoggingEvent event1 = getLoggingEventForTest("Test Exception.", ex);
        LoggingEvent event2 = getLoggingEventForTest("Test Exception.", ex);
        LoggingEvent event3 = getLoggingEventForTest("Test Exception.", ex);
        LoggingEvent event4 = getLoggingEventForTest("Test Exception.", ex);

        appender.append(event1);
        appender.append(event2);

        String logging = os.toString("UTF8");

        String stackTraceKey1 = null;

        // 2016-08-11 11:55:38,487 ERROR [] [SymRollingFileAppenderTest] [main] Test Exception. StackTraceKey.init [Exception:1478675418]
        Pattern initPattern = Pattern.compile(".*StackTraceKey.init \\[Exception:([0-9]*)\\].*", Pattern.DOTALL);
        {
            Matcher m = initPattern.matcher(logging);
            if (m.matches()) {
                {                
                    stackTraceKey1 = "Exception:" + m.group(1);            
                    assertEquals(2, StringUtils.countMatches(logging, stackTraceKey1));
                }
            } else {
                fail("Didn't find proper logging pattern.");
            }
        }

        appender.rollOver();
        os = new ByteArrayOutputStream(1024);
        appender.setOutputStream(os);

        appender.append(event3);
        appender.append(event4);

        logging = os.toString("UTF8");

        String stackTraceKey2 = null;

        {
            Matcher m = initPattern.matcher(logging);
            if (m.matches()) {
                {                
                    stackTraceKey2 = "Exception:" + m.group(1);            
                    assertEquals(2, StringUtils.countMatches(logging, stackTraceKey2));
                }
            } else {
                fail("Didn't find proper logging pattern.");
            }
        }

        // should reprint stack trace and get a new key after the roll over.
        assertEquals(stackTraceKey1, stackTraceKey2);


    }

    @Test
    public void testDebugMode() throws Exception {
        ByteArrayOutputStream os = new ByteArrayOutputStream(1024);
        SymRollingFileAppender appender = getAppenderForTest(os);

        Exception ex = new Exception("Test exception.");

        LoggingEvent event1 = getLoggingEventForTest("Test Exception.", ex, Level.DEBUG);
        LoggingEvent event2 = getLoggingEventForTest("Test Exception.", ex, Level.DEBUG);
        LoggingEvent event3 = getLoggingEventForTest("Test Exception.", ex, Level.DEBUG);

        appender.append(event1);
        appender.append(event2);
        appender.append(event3);

        String logging = os.toString("UTF8");

        // 2016-08-11 11:55:38,487 ERROR [] [SymRollingFileAppenderTest] [main] Test Exception. StackTraceKey.init [Exception:1478675418]
        Pattern initPattern = Pattern.compile(".*StackTraceKey.init \\[Exception:([0-9]*)\\].*", Pattern.DOTALL);
        Matcher m = initPattern.matcher(logging);
        assertFalse(m.matches()); // everything should get logged with a DEBUG logger, so we should't find the StackTraceKey pattern.
    }

    @Test
    public void testCacheExceeded() throws Exception {
        ByteArrayOutputStream os = new ByteArrayOutputStream(1024);
        SymRollingFileAppender appender = getAppenderForTest(os);

        final int HISTORY_SIZE = 100;

        final int TEST_TOTAL = HISTORY_SIZE*4;

        appender.setHistorySize(HISTORY_SIZE);

        for (int i = 0; i < TEST_TOTAL; i++) {            
            Exception ex = new Exception("Test exception."+i);
            LoggingEvent event = getLoggingEventForTest("Test Exception.", ex);
            appender.append(event);
            LoggingEvent event1 = getLoggingEventForTest("Test Exception.", ex);
            appender.append(event1);
        }

        String logging = os.toString("UTF8");

        assertEquals(TEST_TOTAL, StringUtils.countMatches(logging, "StackTraceKey.init"));
    }
    
    protected LoggingEvent getLoggingEventForTest(String message, Throwable ex) {
        return getLoggingEventForTest(message, ex, Level.INFO);
    }

    protected LoggingEvent getLoggingEventForTest(String message, Throwable ex, Level loggerLevel) {
        TestCategory category = new TestCategory(SymRollingFileAppenderTest.class.getName());
        category.setLevel(loggerLevel);

        LoggingEvent event = new LoggingEvent(SymRollingFileAppenderTest.class.getName(), 
                category, System.currentTimeMillis(), Level.ERROR, message, ex);
        return event;
    }

    protected SymRollingFileAppender getAppenderForTest(ByteArrayOutputStream os) throws Exception {
        SymRollingFileAppender appender = new SymRollingFileAppender();
        PatternLayout layout = new PatternLayout("%d %p [%X{engineName}] [%c{1}] [%t] %m%n");
        appender.setLayout(layout);

        appender.setOutputStream(os);
        return appender;
    }

    static class TestCategory extends Category {
        public TestCategory(String name) {
            super(name);
        }
    }
}
