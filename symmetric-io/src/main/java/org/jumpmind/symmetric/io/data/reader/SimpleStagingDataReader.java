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
package org.jumpmind.symmetric.io.data.reader;

import java.io.BufferedReader;
import java.io.BufferedWriter;

import org.apache.commons.io.IOUtils;
import org.jumpmind.exception.IoException;
import org.jumpmind.symmetric.io.data.Batch.BatchType;
import org.jumpmind.symmetric.io.data.DataContext;
import org.jumpmind.symmetric.io.stage.IStagedResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleStagingDataReader {
    final static int MAX_WRITE_LENGTH = 32768;
    
    protected final Logger log = LoggerFactory.getLogger(getClass());

    protected BatchType batchType;
    protected long batchId;
    protected String targetNodeId;
    protected IStagedResource stagedResource;
    protected BufferedWriter writer;
    protected BufferedReader reader;
    protected DataContext context;

    public SimpleStagingDataReader(BatchType batchType, long batchId, String targetNodeId, IStagedResource stagedResource, 
            BufferedWriter writer, DataContext context) {
        this.batchType = batchType;
        this.targetNodeId = targetNodeId;
        this.stagedResource = stagedResource;
        this.writer = writer;
        this.context = context;
    }

    public void process() {
        reader = stagedResource.getReader();
        char[] buffer = new char[MAX_WRITE_LENGTH];
        long totalCharsRead = 0;
        int numCharsRead = 0;
        long startTime = System.currentTimeMillis(), ts = startTime;

        try {
            while ((numCharsRead = reader.read(buffer)) != -1) {
                writer.write(buffer, 0, numCharsRead);
                totalCharsRead += numCharsRead;
                
                if (Thread.currentThread().isInterrupted()) {
                    throw new IoException("This thread was interrupted");
                }
        
                if (System.currentTimeMillis() - ts > 60000) {
                    log.info("Batch '{}', for node '{}', for process 'send from stage' has been processing for {} seconds.  The following stats have been gathered: {}",
                            new Object[] { batchId, targetNodeId, (System.currentTimeMillis() - startTime) / 1000,
                            "BYTES=" + totalCharsRead });
                    ts = System.currentTimeMillis();
                }
            }    
        } catch (Throwable t) {
            throw new RuntimeException(t);
        } finally {
            IOUtils.closeQuietly(reader);
        }
    }
    
}
