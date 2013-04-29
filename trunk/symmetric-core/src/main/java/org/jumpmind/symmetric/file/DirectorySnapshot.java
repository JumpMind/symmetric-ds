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
package org.jumpmind.symmetric.file;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import org.jumpmind.symmetric.model.FileSnapshot;
import org.jumpmind.symmetric.model.FileSnapshot.LastEventType;
import org.jumpmind.symmetric.model.FileTrigger;

public class DirectorySnapshot extends ArrayList<FileSnapshot> {

    private static final long serialVersionUID = 1L;

    private FileTrigger fileTrigger;
    
    public DirectorySnapshot(FileTrigger fileTrigger) {
        this.fileTrigger = fileTrigger;
    }

    protected void merge(DirectorySnapshot snapshot) {
        Set<FileSnapshot> toAdd = new HashSet<FileSnapshot>();
        Set<FileSnapshot> toRemove = new HashSet<FileSnapshot>();
        for (FileSnapshot fileChange : snapshot) {
            for (FileSnapshot file : this) {
                if (fileChange.getFileName().equals(file.getFileName())) {
                    toRemove.add(file);
                    if (fileChange.getLastEventType() == LastEventType.MODIFY) {
                        toAdd.add(fileChange);
                    }
                }
            }
        }
        
        for (FileSnapshot fileChange : toRemove) {
            if (fileChange.getLastEventType() == LastEventType.CREATE) {
                toAdd.add(fileChange);
            }
        }
        
        this.removeAll(toRemove);
        this.addAll(toAdd);
    }
    
    protected DirectorySnapshot diff(DirectorySnapshot snapshot) {
        DirectorySnapshot differences = new DirectorySnapshot(snapshot.getFileTrigger());
        for (FileSnapshot fileChange : snapshot) {
            boolean found = false;
            for (FileSnapshot file : this) {
                found = true;
                if (fileChange.getFileName().equals(file.getFileName())) {
                    if (!fileChange.getLastEventType().equals(file.getLastEventType())) {

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
    
    public FileTrigger getFileTrigger() {
        return fileTrigger;
    }
    
    
}
