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

import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.jumpmind.symmetric.fs.config.DirectorySpec;
import org.jumpmind.symmetric.fs.config.Node;

public class DirectorySpecSnapshot {

    protected Date date;
    protected Node node;
    protected String directory;
    protected DirectorySpec directorySpec;
    protected List<FileChange> files = new ArrayList<FileChange>();

    public DirectorySpecSnapshot(Node node, String directory, DirectorySpec directorySpec) {
        this.date = new Date();
        this.node = node;
        this.directorySpec = directorySpec;
    }
    
    public DirectorySpec getDirectorySpec() {
        return directorySpec;
    }
    
    public List<FileChange> getFiles() {
        return files;
    }
    
    public void addFileChange(FileChange fileChange) {
        files.add(fileChange);
    }

    protected void merge(DirectorySpecSnapshot snapshot) {
       merge(snapshot.getFiles());
    }
    
    protected void merge(List<FileChange> changes) {
        Set<FileChange> toAdd = new HashSet<FileChange>();
        Set<FileChange> toRemove = new HashSet<FileChange>();
        for (FileChange fileChange : changes) {
            for (FileChange file : files) {
                if (fileChange.getFileName().equals(file.getFileName())) {
                    toRemove.add(file);
                    if (fileChange.getFileChangeType() == FileChangeType.UPDATE) {
                        toAdd.add(fileChange);
                    }
                }
            }
        }
        
        for (FileChange fileChange : toRemove) {
            if (fileChange.getFileChangeType() == FileChangeType.CREATE) {
                toAdd.add(fileChange);
            }
        }
        
        files.removeAll(toRemove);
        files.addAll(toAdd);
    }
    
    protected List<FileChange> diff(DirectorySpecSnapshot snapshot) {
        List<FileChange> differences = new ArrayList<FileChange>();
        for (FileChange fileChange : snapshot.getFiles()) {
            boolean found = false;
            for (FileChange file : files) {
                found = true;
                if (fileChange.getFileName().equals(file.getFileName())) {
                    if (!fileChange.getFileChangeType().equals(file.getFileChangeType())) {

                        differences.add(file);
                    }
                }
            }
            if (!found) {
                differences.add(fileChange);
            }
        }
        return differences;
    }
    
    public String getDirectory() {
        return directory;
    }

}
