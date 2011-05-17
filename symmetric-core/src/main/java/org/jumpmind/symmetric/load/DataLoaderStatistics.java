/*
 * Licensed to JumpMind Inc under one or more contributor 
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding 
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU Lesser General Public License (the
 * "License"); you may not use this file except in compliance
 * with the License. 
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see           
 * <http://www.gnu.org/licenses/>.
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.  */
package org.jumpmind.symmetric.load;

import java.util.Date;

/**
 * Utility class used by the data loader to record statistics as data is being loaded.
 */
public class DataLoaderStatistics implements IDataLoaderStatistics {

    private Date startTime;

    private long filterMillis;

    private long databaseMillis;

    private long byteCount;

    private long lineCount;

    private long statementCount;

    private long fallbackInsertCount;

    private long fallbackUpdateCount;

    private long missingDeleteCount;

    private long timerMillis;

    public DataLoaderStatistics() {
        this.startTime = new Date();
    }

    public long incrementLineCount() {
        return ++lineCount;
    }

    public long incrementFallbackInsertCount() {
        return ++fallbackInsertCount;
    }

    public long incrementFallbackUpdateCount() {
        return ++fallbackUpdateCount;
    }

    public long incrementMissingDeleteCount() {
        return ++missingDeleteCount;
    }

    public long incrementStatementCount() {
        return ++statementCount;
    }

    public void incrementFilterMillis(long millis) {
        filterMillis += millis;
    }

    public void incrementDatabaseMillis(long millis) {
        databaseMillis += millis;
    }

    public void incrementByteCount(long count) {
        byteCount += count;
    }

    public void startTimer() {
        timerMillis = System.currentTimeMillis();
    }

    public long endTimer() {
        return System.currentTimeMillis() - timerMillis;
    }

    public long getFallbackInsertCount() {
        return fallbackInsertCount;
    }

    public void setFallbackInsertCount(long fallbackInsertCount) {
        this.fallbackInsertCount = fallbackInsertCount;
    }

    public long getFallbackUpdateCount() {
        return fallbackUpdateCount;
    }

    public void setFallbackUpdateCount(long fallbackUpdateCount) {
        this.fallbackUpdateCount = fallbackUpdateCount;
    }

    public long getLineCount() {
        return lineCount;
    }

    public void setLineCount(long lineCount) {
        this.lineCount = lineCount;
    }

    public Date getStartTime() {
        return startTime;
    }

    public void setStartTime(Date startTime) {
        this.startTime = startTime;
    }

    public long getStatementCount() {
        return statementCount;
    }

    public void setStatementCount(long statementCount) {
        this.statementCount = statementCount;
    }

    public long getMissingDeleteCount() {
        return missingDeleteCount;
    }

    public void setMissingDeleteCount(long missingDeleteCount) {
        this.missingDeleteCount = missingDeleteCount;
    }

    public long getDatabaseMillis() {
        return databaseMillis;
    }

    public void setDatabaseMillis(long databaseMillis) {
        this.databaseMillis = databaseMillis;
    }

    public long getFilterMillis() {
        return filterMillis;
    }

    public void setFilterMillis(long filterMillis) {
        this.filterMillis = filterMillis;
    }

    public long getByteCount() {
        return byteCount;
    }

    public void setByteCount(long byteCount) {
        this.byteCount = byteCount;
    }

}