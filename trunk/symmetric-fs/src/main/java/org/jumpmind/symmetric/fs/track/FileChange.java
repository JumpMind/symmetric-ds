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
package org.jumpmind.symmetric.fs.track;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.jumpmind.exception.IoException;
import org.jumpmind.symmetric.fs.util.Utils;

public class FileChange {

    protected String fileName;
    protected FileChangeType fileChangeType;
    protected String hashCode;

    public FileChange(File baseDir, File file, FileChangeType fileChangeType) {
        this.fileChangeType = fileChangeType;
        this.fileName = Utils.getRelativePath(file.getPath(), baseDir.getPath()); 
        if (file.isFile()) {
            try {
                this.hashCode = Long.toString(FileUtils.checksumCRC32(file));
            } catch (IOException ex) {
                throw new IoException(ex);
            }
        } else {
            hashCode = "";
        }
    }

    public String getFileName() {
        return fileName;
    }

    public FileChangeType getFileChangeType() {
        return fileChangeType;
    }

    public String getHashCode() {
        return hashCode;
    }

}
