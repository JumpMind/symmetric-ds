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
 * under the License. 
 */
package org.jumpmind.symmetric.fs.client;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.jumpmind.symmetric.fs.config.Node;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonIOException;
import com.google.gson.stream.JsonWriter;

public class FileSystemSyncStatusPersister implements ISyncStatusPersister {

    static final String SUFFIX_WRITING = "writing";
    static final String SUFFIX_STATUS = "status";

    String directory;

    public FileSystemSyncStatusPersister(String directory) {
        this.directory = directory;
        new File(directory).mkdirs();
    }

    public SyncStatus get(Node node) {
        SyncStatus status = null;
        File file = new File(directory, String.format("%s.%s", node.getNodeId(), SUFFIX_STATUS));
        if (!file.exists()) {
            file = new File(directory, String.format("%s.%s", node.getNodeId(), SUFFIX_WRITING));
        }

        if (file.exists()) {
            FileReader reader = null;
            try {
                reader = new FileReader(file);
                Gson gson = new GsonBuilder().serializeNulls().setPrettyPrinting().create();
                status = gson.fromJson(reader, SyncStatus.class);
            } catch (IOException ex) {
                throw new JsonIOException(ex);
            } finally {
                IOUtils.closeQuietly(reader);
            }
        }
        return status;
    }

    public void save(SyncStatus status) {
        File writingFile = new File(directory, String.format("%s.%s", status.getNode().getNodeId(),
                SUFFIX_WRITING));
        File finalFile = new File(directory, String.format("%s.%s", status.getNode().getNodeId(),
                SUFFIX_STATUS));
        if (writingFile.exists()) {
            FileUtils.deleteQuietly(writingFile);
        }
        Gson gson = new GsonBuilder().serializeNulls().setPrettyPrinting().create();
        JsonWriter writer = null;
        try {
            writer = new JsonWriter(new FileWriter(writingFile));
            writer.setSerializeNulls(true);
            writer.setIndent("  ");
            gson.toJson(status, SyncStatus.class, writer);
            if (finalFile.exists()) {
                FileUtils.deleteQuietly(finalFile);
            }
            FileUtils.moveFile(writingFile, finalFile);
        } catch (IOException ex) {
            throw new JsonIOException(ex);
        } finally {
            IOUtils.closeQuietly(writer);
        }
    }
}
