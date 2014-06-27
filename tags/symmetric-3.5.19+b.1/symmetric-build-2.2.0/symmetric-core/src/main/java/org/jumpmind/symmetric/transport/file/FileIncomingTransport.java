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

package org.jumpmind.symmetric.transport.file;

import java.io.BufferedReader;
import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.jumpmind.symmetric.io.ThresholdFileWriter;
import org.jumpmind.symmetric.transport.IIncomingTransport;

/**
 * Wraps an incoming stream that comes from a file.
 */
public class FileIncomingTransport implements IIncomingTransport {

    ThresholdFileWriter fileWriter;

    BufferedReader reader;

    public FileIncomingTransport(ThresholdFileWriter fileWriter) {
        this.fileWriter = fileWriter;
    }

    public void close() throws IOException {
        IOUtils.closeQuietly(reader);
        if (fileWriter != null) {
            fileWriter.delete();
        }
    }

    public boolean isOpen() {
        return reader != null;
    }

    public BufferedReader open() throws IOException {
        reader = fileWriter.getReader();
        return reader;
    }
    
    public String getRedirectionUrl() {
        return null;
    }

}