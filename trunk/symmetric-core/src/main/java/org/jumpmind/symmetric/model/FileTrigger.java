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
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.io.filefilter.AndFileFilter;
import org.apache.commons.io.filefilter.FileFilterUtils;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.apache.commons.io.filefilter.NotFileFilter;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.apache.commons.lang.StringUtils;

public class FileTrigger implements Serializable {

    private static final long serialVersionUID = 1L;
    
    private String triggerId;
    private String baseDir;
    private boolean recursive;
    private String includesFiles;
    private String excludesFiles;
    private boolean syncOnCreate = true;
    private boolean syncOnModified = true;
    private boolean syncOnDelete = true;
    private String beforeCopyScript;
    private String afterCopyScript;
    private Date createTime = new Date();
    private String lastUpdateBy;
    private Date lastUpdateTime;

    public FileTrigger() {
    }
    
    public FileTrigger(String baseDir, boolean recursive, String includes, String excludes) {
        this.baseDir = baseDir;
        this.recursive = recursive;
        this.includesFiles = includes;
        this.excludesFiles = excludes;
        this.triggerId = "?";
    }
    
    
    public String getTriggerId() {
        return triggerId;
    }

    public void setTriggerId(String triggerId) {
        this.triggerId = triggerId;
    }

    public String getBaseDir() {
        return baseDir;
    }

    public void setBaseDir(String baseDir) {
        this.baseDir = baseDir;
    }

    public boolean isRecursive() {
        return recursive;
    }

    public void setRecursive(boolean recursive) {
        this.recursive = recursive;
    }

    public String getIncludesFiles() {
        return includesFiles;
    }

    public void setIncludesFiles(String includesFiles) {
        this.includesFiles = includesFiles;
    }

    public String getExcludesFiles() {
        return excludesFiles;
    }

    public void setExcludesFiles(String excludesFiles) {
        this.excludesFiles = excludesFiles;
    }

    public boolean isSyncOnCreate() {
        return syncOnCreate;
    }

    public void setSyncOnCreate(boolean syncOnCreate) {
        this.syncOnCreate = syncOnCreate;
    }

    public boolean isSyncOnModified() {
        return syncOnModified;
    }

    public void setSyncOnModified(boolean syncOnModified) {
        this.syncOnModified = syncOnModified;
    }

    public boolean isSyncOnDelete() {
        return syncOnDelete;
    }

    public void setSyncOnDelete(boolean syncOnDelete) {
        this.syncOnDelete = syncOnDelete;
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
    
    public IOFileFilter createIOFileFilter() {        
        String[] includes = StringUtils.isNotBlank(includesFiles) ? includesFiles.split(",") : new String[] {"*"};
        String[] excludes = StringUtils.isNotBlank(excludesFiles) ? excludesFiles.split(",") : null;
        IOFileFilter filter = new WildcardFileFilter(includes);
        if (excludes != null && excludes.length > 0) {
            List<IOFileFilter> fileFilters = new ArrayList<IOFileFilter>();
            fileFilters.add(filter);
            fileFilters.add(new NotFileFilter(new WildcardFileFilter(excludes)));
            filter = new AndFileFilter(fileFilters);
        }
        if (!recursive) {
            List<IOFileFilter> fileFilters = new ArrayList<IOFileFilter>();
            fileFilters.add(filter);
            fileFilters.add(new NotFileFilter(FileFilterUtils.directoryFileFilter()));
            filter = new AndFileFilter(fileFilters);            
        }
        return filter;
    }
    
    public void setAfterCopyScript(String afterCopyScript) {
        this.afterCopyScript = afterCopyScript;
    }
    
    public String getAfterCopyScript() {
        return afterCopyScript;
    }
    
    public void setBeforeCopyScript(String beforeCopyScript) {
        this.beforeCopyScript = beforeCopyScript;
    }
    
    public String getBeforeCopyScript() {
        return beforeCopyScript;
    }
    
    public File createSourceFile(FileSnapshot snapshot) {
        File sourceBaseDir = new File(baseDir);
        if (!snapshot.getFilePath().equals(".")) {
            String sourcePath = snapshot.getFilePath() + "/";
            sourceBaseDir = new File(sourceBaseDir, sourcePath);
        }
        return new File(sourceBaseDir, snapshot.getFileName());
    }
    
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof FileTrigger && triggerId != null) {
            return triggerId.equals(((FileTrigger) obj).triggerId);
        } else {
            return super.equals(obj);
        }
    }    
    
    @Override
    public int hashCode() {
        return triggerId != null ? triggerId.hashCode() : super.hashCode();
    }

    @Override
    public String toString() {
        if (triggerId != null) {
            return triggerId;
        } else {
            return super.toString();
        }
    }

}
