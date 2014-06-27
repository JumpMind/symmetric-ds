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
package org.jumpmind.symmetric.model;

import java.io.Serializable;

public class DataEvent implements Serializable {
    
    private static final long serialVersionUID = 1L;

    private long dataId;
   
    private long batchId;
    
    private String routerId;

    public DataEvent() {
    }

    public DataEvent(long dataId, long batchId, String routerId) {
        this.dataId = dataId;
        this.batchId = batchId;
        this.routerId = routerId;
    }
    
    public long getBatchId() {
        return batchId;
    }

    public void setBatchId(long batchId) {
        this.batchId = batchId;
    }

    public long getDataId() {
        return dataId;
    }

    public void setDataId(long dataId) {
        this.dataId = dataId;
    }

    public void setRouterId(String routerId) {
        this.routerId = routerId;
    }
    
    public String getRouterId() {
        return routerId;
    }
}