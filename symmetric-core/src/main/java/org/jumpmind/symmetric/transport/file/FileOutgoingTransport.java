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
package org.jumpmind.symmetric.transport.file;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.StringReader;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.jumpmind.symmetric.io.data.CsvConstants;
import org.jumpmind.symmetric.model.ChannelMap;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.service.IConfigurationService;
import org.jumpmind.symmetric.transport.IOutgoingWithResponseTransport;
import org.jumpmind.symmetric.web.WebConstants;

public class FileOutgoingTransport implements IOutgoingWithResponseTransport {

    String fileName;

    BatchBufferedWriter writer;
    
    OutputStream out;

    boolean open = true;
    
    public FileOutgoingTransport(Node remoteNode, Node localNode, String outgoingDir) throws IOException {
        this.fileName = outgoingDir + File.separator + localNode.getNodeGroupId() + "-" + localNode.getNodeId() + "_to_" + 
                remoteNode.getNodeGroupId() + "-" + remoteNode.getNodeId() + "_" + System.currentTimeMillis();
    }

    @Override
    public BufferedWriter openWriter() {
        try {
            writer = new BatchBufferedWriter(new FileWriter(fileName + ".tmp"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return writer;
    }

    @Override
    public OutputStream openStream() {
        try {
            out = new FileOutputStream(fileName + ".tmp");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return out;
    }

    @Override
    public BufferedReader readResponse() throws IOException {
        StringBuilder resp = new StringBuilder();
        for (String batchId : writer.getBatchIds()) {
            resp.append(WebConstants.ACK_BATCH_NAME).append(batchId).append("=").append(WebConstants.ACK_BATCH_OK).append("&");
        }
        return new BufferedReader(new StringReader(resp.toString()));
    }

    @Override
    public void close() {
        IOUtils.closeQuietly(writer);
        IOUtils.closeQuietly(out);
        open = false;
    }

    @Override
    public boolean isOpen() {
        return open;
    }

    @Override
    public ChannelMap getSuspendIgnoreChannelLists(IConfigurationService configurationService, Node targetNode) {
        return configurationService.getSuspendIgnoreChannelLists();
    }

    public void complete(boolean success) {
        if (!success) {
            new File(fileName).delete();
        } else if (writer != null) {
            new File(fileName + ".tmp").renameTo(new File(fileName + ".csv"));
        } else {
            new File(fileName + ".tmp").renameTo(new File(fileName + ".zip"));
        }
    }

    class BatchBufferedWriter extends BufferedWriter {
        List<String> batchIds = new ArrayList<String>();
        boolean isCommit = false;

        public BatchBufferedWriter(Writer out) {
            super(out);
        }

        public void write(String str) throws IOException {
            super.write(str);
            if (str.equals(CsvConstants.COMMIT)) {
                isCommit = true;
            } else if (!str.equals(",")) {
                if (isCommit) {
                    batchIds.add(str);    
                }
                isCommit = false;
            }
        }
        
        public List<String> getBatchIds() {
            return batchIds;
        }
    }
}
