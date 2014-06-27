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
package org.jumpmind.symmetric.model;

import java.io.Serializable;
import java.util.Date;

public class DataGap implements Serializable {
    
    private static final long serialVersionUID = 1L;

    public enum Status {GP,SK,OK};
    
    private long startId;
    private long endId;
    private Date createTime;

    public DataGap(long startId, long endId) {
        this.startId = startId;
        this.endId = endId;
        this.createTime = new Date();
    }
    
    public DataGap(long startId, long endId, Date createTime) {
        this.startId = startId;
        this.endId = endId;
        this.createTime = createTime;
    }

    public void setEndId(long endId) {
        this.endId = endId;
    }
    
    public long getEndId() {
        return endId;
    }
    
    public void setStartId(long startId) {
        this.startId = startId;
    }
    
    public long getStartId() {
        return startId;
    }
    
    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }
    
    public Date getCreateTime() {
        return createTime;
    }
    
    public boolean contains(DataGap gap) {
        return startId <= gap.startId && endId >= gap.endId;
    }
    
    public long gapSize() {
    	return endId-startId;
    }
    
}