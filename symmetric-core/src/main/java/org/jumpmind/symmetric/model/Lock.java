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

import java.io.Serializable;
import java.util.Date;

public class Lock implements Serializable {

    public static final String STOPPED = "STOPPED";
    
    private static final long serialVersionUID = 1L;

    private String lockAction;
    private String lockingServerId;
    private Date lockTime;
    private Date lastLockTime;
    private String lastLockingServerId;

    public String getLockAction() {
        return lockAction;
    }
    
    public boolean isStopped() {
       return STOPPED.equals(lockingServerId) && lockTime != null;
    }
    
    public boolean isLockedByOther(String serverId) {
        return lockTime != null  && lockingServerId != null && !lockingServerId.equals(serverId);
    }

    public void setLockAction(String lockAction) {
        this.lockAction = lockAction;
    }

    public String getLockingServerId() {
        return lockingServerId;
    }

    public void setLockingServerId(String lockingServerId) {
        this.lockingServerId = lockingServerId;
    }

    public Date getLockTime() {
        return lockTime;
    }

    public void setLockTime(Date lockTime) {
        this.lockTime = lockTime;
    }

    public Date getLastLockTime() {
        return lastLockTime;
    }

    public void setLastLockTime(Date lastLockTime) {
        this.lastLockTime = lastLockTime;
    }

    public String getLastLockingServerId() {
        return lastLockingServerId;
    }

    public void setLastLockingServerId(String lastLockingServerId) {
        this.lastLockingServerId = lastLockingServerId;
    }

}
