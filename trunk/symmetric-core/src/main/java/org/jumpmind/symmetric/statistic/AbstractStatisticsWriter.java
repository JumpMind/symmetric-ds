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
package org.jumpmind.symmetric.statistic;

import java.io.FilterWriter;
import java.io.IOException;
import java.io.Writer;

abstract class AbstractStatisticsWriter extends FilterWriter {

    protected IStatisticManager statisticManager;
    
    protected String channelId;

    protected long byteCount = 0;

    protected long lineCount = 0;

    private int notifyAfterByteCount;

    private int notifyAfterLineCount;

    public AbstractStatisticsWriter(IStatisticManager statisticManager, Writer out,
            int notifyAfterByteCount, int notifyAfterLineCount) {
        super(out);
        this.statisticManager = statisticManager;
        this.notifyAfterByteCount = notifyAfterByteCount;
        this.notifyAfterLineCount = notifyAfterLineCount;
    }
    
    public void setChannelId(String channelId) {
        this.channelId = channelId;
        flushCounts();
    }

    @Override
    public void write(int c) throws IOException {
        super.write(c);
        byteCount++;
        if (byteCount % notifyAfterByteCount == 0) {
            processNumberOfBytesSoFar(byteCount);
            byteCount = 0;
        }
        if (((char) c) == '\n') {
            lineCount++;
            if (lineCount % notifyAfterLineCount == 0) {
                processNumberOfLinesSoFar(lineCount);
                lineCount = 0;
            }
        }
    }
    
    @Override
    public void write(char[] cbuf, int off, int len) throws IOException {       
        super.write(cbuf, off, len);
        byteCount+=len;
        if (byteCount % notifyAfterByteCount == 0) {
            processNumberOfBytesSoFar(byteCount);
            byteCount = 0;
        }
        
        for (int i = 0; i < len; i++) {
            char c = cbuf[i+off];
            if (((char) c) == '\n') {
                lineCount++;
                if (lineCount % notifyAfterLineCount == 0) {
                    processNumberOfLinesSoFar(lineCount);
                    lineCount = 0;
                }
            }
        }
        
    }   
    
    @Override
    public void write(String str, int off, int len) throws IOException {
        super.write(str, off, len);
        byteCount+=len;
        if (byteCount % notifyAfterByteCount == 0) {
            processNumberOfBytesSoFar(byteCount);
            byteCount = 0;
        }
        
        for (int i = 0; i < len; i++) {
            char c = str.charAt(i+off);
            if (((char) c) == '\n') {
                lineCount++;
                if (lineCount % notifyAfterLineCount == 0) {
                    processNumberOfLinesSoFar(lineCount);
                    lineCount = 0;
                }
            }
        }
    }
    
    protected void flushCounts() {
        if (lineCount > 0) {
            processNumberOfLinesSoFar(lineCount);
            lineCount = 0;
        }
        if (byteCount > 0) {
            processNumberOfBytesSoFar(byteCount);
            byteCount = 0;
        }
    }
    
    @Override
    public void flush() throws IOException {
        super.flush();
        flushCounts();
    }

    @Override
    public void close() throws IOException {
        super.close();
        flushCounts();
    }

    abstract protected void processNumberOfBytesSoFar(long count);

    abstract protected void processNumberOfLinesSoFar(long count);

}