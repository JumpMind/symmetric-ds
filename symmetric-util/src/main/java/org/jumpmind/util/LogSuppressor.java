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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Level;
import org.slf4j.Logger;

public class LogSuppressor {

    private int reportMessageXTimes;
    
    protected final Logger log;
    
    private Map<String, Integer> logCounts = Collections.synchronizedMap(new LinkedHashMap<String, Integer>() {
        private static final long serialVersionUID = 1L;

        @Override
        protected boolean removeEldestEntry(Entry<String, Integer> eldest) {
            return (this.size() > 2048);
        }
    });
    
    public LogSuppressor(Logger log) {
        this(log, 10);
    }
    
    public LogSuppressor(Logger log, int reportMessageXTimes) {
        this.log = log;
        this.reportMessageXTimes = reportMessageXTimes;
    }
    
    protected void log(int initialLevel, String key, String message, Throwable ex) {
        if (!logCounts.containsKey(key)) {   
            if (initialLevel == Level.ERROR_INT) {
                if (ex != null) {                    
                    log.error(message, ex);    
                } else {
                    log.error(message);
                }
            } else {
                if (ex != null) {                    
                    log.info(message, ex);    
                } else {
                    log.info(message);
                }   
            }
            
            logCounts.put(key, Integer.valueOf(1));
        } else {
            String exceptionText = ex != null ? " "+ex.getMessage() : "";
            
            Integer count = logCounts.get(key);
            String messagePreviouslyReported = message + " (Previously reported " + count + " time(s), will report " + reportMessageXTimes + ")"; 
            if (count < reportMessageXTimes) {
                if (initialLevel == Level.ERROR_INT) {                    
                    log.error(messagePreviouslyReported + exceptionText);                        
                } else {
                    log.info(messagePreviouslyReported + exceptionText);
                }
                
            } else {
                log.debug(messagePreviouslyReported + exceptionText);                                        
            }
            count = count+1;
            logCounts.put(key, count);
        }
    }

    public void logError(String key, String message, Throwable ex) {
        log(Level.ERROR_INT, key, message, ex);
    }
    
    public void logInfo(String key, String message, Throwable ex) {
        log(Level.INFO_INT, key, message, ex);
    }
}
