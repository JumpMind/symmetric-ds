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
package org.jumpmind.symmetric.model;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.Date;

import org.apache.commons.io.FileUtils;
import org.jumpmind.exception.IoException;

public class FileSnapshot implements Serializable {

    private static final long serialVersionUID = 1L;

    public enum LastEventType {
        CREATE("C"), MODIFY("M"), DELETE("D"), SEED("S");
        private LastEventType(String code) {
            this.code = code;
        }

        private String code;

        public String getCode() {
            return this.code;
        }
        
        public static LastEventType fromCode(String code) {
            if ("C".equals(code)) {
                return CREATE;
            } else if ("M".equals(code)) {
                return MODIFY;
                
            } else if ("D".equals(code)) {
                return DELETE;
                
            } else if ("S".equals(code)) {
                return SEED;
                
            } else {
                return null;
            }
        }
    };

    private String triggerId;
    private String filePath;
    private String fileName;
    private LastEventType lastEventType;
    private long crc32Checksum;
    private long fileSize;
    private Date fileModifiedTime;
    private Date createTime = new Date();
    private String lastUpdateBy;
    private Date lastUpdateTime;

    public FileSnapshot() {
    }

    public FileSnapshot(FileTrigger fileTrigger, File file, LastEventType lastEventType) {
        this.triggerId = fileTrigger.getTriggerId();
        this.lastEventType = lastEventType;
        this.lastUpdateTime = new Date();
        this.fileName = file.getName();
        this.filePath = file.getAbsolutePath();
        if (this.filePath.startsWith(fileTrigger.getBaseDir())) {
            this.filePath = this.filePath.substring(0, fileTrigger.getBaseDir().length() - 1);
        }

        if (this.filePath.endsWith(fileName)) {
            this.filePath = this.filePath.substring(this.filePath.indexOf(fileName),
                    this.filePath.length());
        }

        this.fileSize = file.length();
        this.fileModifiedTime = new Date(file.lastModified());
        if (file.isFile() && lastEventType != LastEventType.DELETE) {
            try {
                this.crc32Checksum = FileUtils.checksumCRC32(file);
            } catch (FileNotFoundException ex) {
                this.lastEventType = LastEventType.DELETE;
            } catch (IOException ex) {
                throw new IoException(ex);
            }
        } else {
            this.crc32Checksum = -1;
        }

    }

    public String getTriggerId() {
        return triggerId;
    }

    public void setTriggerId(String triggerId) {
        this.triggerId = triggerId;
    }

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public LastEventType getLastEventType() {
        return lastEventType;
    }

    public void setLastEventType(LastEventType lastEventType) {
        this.lastEventType = lastEventType;
    }

    public long getCrc32Checksum() {
        return crc32Checksum;
    }

    public void setCrc32Checksum(long crc32Checksum) {
        this.crc32Checksum = crc32Checksum;
    }

    public long getFileSize() {
        return fileSize;
    }

    public void setFileSize(long fileSize) {
        this.fileSize = fileSize;
    }

    public Date getFileModifiedTime() {
        return fileModifiedTime;
    }

    public void setFileModifiedTime(Date fileModifiedTime) {
        this.fileModifiedTime = fileModifiedTime;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

    public String getLastUpdateBy() {
        return lastUpdateBy;
    }

    public void setLastUpdateBy(String lastUpdateBy) {
        this.lastUpdateBy = lastUpdateBy;
    }

    public Date getLastUpdateTime() {
        return lastUpdateTime;
    }

    public void setLastUpdateTime(Date lastUpdateTime) {
        this.lastUpdateTime = lastUpdateTime;
    }

}
