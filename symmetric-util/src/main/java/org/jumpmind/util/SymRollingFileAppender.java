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

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.zip.CRC32;

import org.apache.commons.lang.ArrayUtils;
import org.apache.log4j.Level;
import org.apache.log4j.RollingFileAppender;
import org.apache.log4j.helpers.LogLog;
import org.apache.log4j.spi.LoggingEvent;

public class SymRollingFileAppender extends RollingFileAppender {

    private static final int DEFAULT_HISTORY_SIZE = 2048; 

    // All access to this field should come from method calls that are otherwise synchronized 
    // (e.g. AppenderSkeleton.doAppend())
    private Map<String, String> loggedEventKeys = new LinkedHashMap<String, String>() {
        private static final long serialVersionUID = 1L;
        
        @Override
        protected boolean removeEldestEntry(Entry<String, String> eldest) {
            return (size() >= getHistorySize());
        }
    };

    private OutputStream os; // for unit testing.
    
    private int historySize = DEFAULT_HISTORY_SIZE;

    @Override
    public void rollOver() {
        loggedEventKeys.clear();
        super.rollOver();
    }

    // Note that this is called from AppenderSkeleton.doAppend() which is synchronized.
    @Override
    public void append(LoggingEvent event) {
        if (!isLoggerAtDebug(event)) { // don't filter logging at all if the logger is at DEBUG level.
            String key = toKey(event);
            if (key != null) {            
                if (loggedEventKeys.containsKey(key)) {
                    event = supressStackTrace(event, key);
                } else {
                    event = appendKey(event, key);
                    loggedEventKeys.put(key, null);                    
                }
            }
        }

        super.append(event);
    }

    /**
     * @param event
     * @return
     */
    private boolean isLoggerAtDebug(LoggingEvent event) {
        if (event != null && event.getLogger() != null && event.getLogger().getLevel() != null) {
            return event.getLogger().getLevel().toInt() <= Level.DEBUG_INT;
        }
        return false;
    }

    protected String toKey(LoggingEvent event) {
        if (event.getThrowableInformation() == null 
                || event.getThrowableStrRep() == null) {
            return null;
        }

        try {
            StringBuilder buff = new StringBuilder(128);
            Throwable throwable = event.getThrowableInformation().getThrowable();
            buff.append(throwable.getClass().getSimpleName());
            if (throwable.getStackTrace().length == 0) {
                buff.append("-jvm-optimized");    
            }
            buff.append(":");
            buff.append(getThrowableHash(event.getThrowableStrRep()));
            return buff.toString();
        } catch (Exception ex) {
            LogLog.error("Failed to hash stack trace.", ex);
            return null;
        }
    }
    
    protected long getThrowableHash(String[] throwableString) throws UnsupportedEncodingException {
        CRC32 crc = new CRC32();
        crc.update(ArrayUtils.toString(throwableString).getBytes("UTF8"));
        return crc.getValue();
    }

    protected LoggingEvent appendKey(LoggingEvent event, String key) {
        String message = getMessageWithKey(event, key, ".init");

        LoggingEvent eventClone = new LoggingEvent(event.getFQNOfLoggerClass(), 
                event.getLogger(), event.getTimeStamp(), event.getLevel(), message, event.getThreadName(), 
                event.getThrowableInformation(), event.getNDC(), event.getLocationInformation(), event.getProperties());

        return eventClone;
    }    

    protected LoggingEvent supressStackTrace(LoggingEvent event, String key) {
        String message = getMessageWithKey(event, key);

        LoggingEvent eventClone = new LoggingEvent(event.getFQNOfLoggerClass(), 
                event.getLogger(), event.getTimeStamp(), event.getLevel(), message, event.getThreadName(), 
                null, event.getNDC(), event.getLocationInformation(), event.getProperties());

        return eventClone;
    }

    protected String getMessageWithKey(LoggingEvent event, String key) {
        return getMessageWithKey(event, key, null);
    }

    protected String getMessageWithKey(LoggingEvent event, String key, String prefix) {
        StringBuilder buff = new StringBuilder(128);
        if (event.getMessage() != null) {
            buff.append(event.getMessage()).append(" ");
        }
        buff.append("StackTraceKey");
        if (prefix != null) {
            buff.append(prefix);
        }
        buff.append(" [").append(key).append("]");
        return buff.toString();
    }

    @Override
    protected void writeHeader() {
        if(layout != null) {
            String h = layout.getHeader();
            if(h != null && this.qw != null)
                this.qw.write(h);
        }
    }

    @Override
    public synchronized void setFile(String fileName, boolean append, boolean bufferedIO, int bufferSize)
            throws IOException {
        if (os == null) {
            super.setFile(fileName, append, bufferedIO, bufferSize);            
        }
    }

    // For unit testing.
    public synchronized void setOutputStream(OutputStream os) throws IOException {
        this.os = os;
        reset();

        Writer fw = createWriter(os);
        if(bufferedIO) {
            fw = new BufferedWriter(fw, bufferSize);
        }
        this.setQWForFiles(fw);
        this.fileName = os.toString();
        this.fileAppend = true;
        writeHeader();
    }
    
    public int getHistorySize() {
        return historySize;
    }

    public void setHistorySize(int historySize) {
        this.historySize = historySize;
    }
}
