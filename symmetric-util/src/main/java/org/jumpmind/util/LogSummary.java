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

import java.io.PrintWriter;
import java.io.StringWriter;

import org.apache.log4j.Level;

public class LogSummary implements Comparable<LogSummary> {

    
    private Level level;

    private String mostRecentThreadName;

    private Throwable throwable;

    private long firstOccurranceTime;

    private long mostRecentTime;

    private int count;

    private String message;

    private String stackTrace;
    
    private Integer levelInt;
    
    public void setLevel(Level level) {
        this.level = level;
    }

    public Level getLevel() {
        return level;
    }

    public String getStackTrace() {
        if (this.stackTrace == null && this.throwable != null) {
            StringWriter st = new StringWriter();
            throwable.printStackTrace(new PrintWriter(st));
            this.stackTrace = st.toString();
        }
        return this.stackTrace;
    }
    
    public void setStackTrace(String st) {
        this.stackTrace = st;
    }

    public Integer getLevelInt() {
        return this.levelInt != null ? this.levelInt : this.level != null ? this.level.toInt() : 0;
    }

    public void setLevelInt(Integer levelInt) {
        this.levelInt = levelInt;
    }

    public long getFirstOccurranceTime() {
        return firstOccurranceTime;
    }

    public void setFirstOccurranceTime(long firstOccurranceDate) {
        this.firstOccurranceTime = firstOccurranceDate;
    }

    public long getMostRecentTime() {
        return mostRecentTime;
    }

    public void setMostRecentTime(long mostRecentDate) {
        this.mostRecentTime = mostRecentDate;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public void setMostRecentThreadName(String mostRecentThreadName) {
        this.mostRecentThreadName = mostRecentThreadName;
    }

    public String getMostRecentThreadName() {
        return mostRecentThreadName;
    }

    public void setThrowable(Throwable throwable) {
        this.throwable = throwable;
    }

    public Throwable getThrowable() {
        return throwable;
    }
    
    @Override
    public int compareTo(LogSummary other) {
        if (mostRecentTime == other.mostRecentTime) {
            return 0;
        } else {
            return mostRecentTime > other.mostRecentTime ? 1 : -1;
        }
    }
    
    
    
}
