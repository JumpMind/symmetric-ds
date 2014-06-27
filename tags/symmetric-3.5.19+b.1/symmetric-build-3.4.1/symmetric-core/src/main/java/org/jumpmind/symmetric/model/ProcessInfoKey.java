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

public class ProcessInfoKey implements Serializable {

    private static final long serialVersionUID = 1L;

    public enum ProcessType {
        PUSH_JOB, PULL_JOB, PUSH_HANDLER, PULL_HANDLER, ROUTER_JOB, GAP_DETECT, ROUTER_READER, MANUAL_LOAD, REGISTRATION_ATTEMPT, REGISTRATION_HANDLER;
        
        public String toString() {
            switch (this) {
                case MANUAL_LOAD:
                    return "Manual Load";
                case PUSH_JOB:
                    return "Push";
                case PULL_JOB:
                    return "Pull";
                case PUSH_HANDLER:
                    return "Service Push";
                case PULL_HANDLER:
                    return "Service Pull";
                case ROUTER_JOB:
                    return "Routing";
                case ROUTER_READER:
                    return "Routing Reader";
                case GAP_DETECT:
                    return "Gap Detection";
                case REGISTRATION_ATTEMPT:
                    return "Registration Attempt";
                case REGISTRATION_HANDLER:
                    return "Service Registration";
                default:
                    return name();
            }
        }
    };

    private String sourceNodeId;

    private String targetNodeId;

    private ProcessType processType;

    public ProcessInfoKey(String sourceNodeId, String targetNodeId, ProcessType processType) {
        this.sourceNodeId = sourceNodeId;
        this.targetNodeId = targetNodeId;
        this.processType = processType;
    }

    public String getSourceNodeId() {
        return sourceNodeId;
    }

    public String getTargetNodeId() {
        return targetNodeId;
    }

    public ProcessType getProcessType() {
        return processType;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((processType == null) ? 0 : processType.hashCode());
        result = prime * result + ((sourceNodeId == null) ? 0 : sourceNodeId.hashCode());
        result = prime * result + ((targetNodeId == null) ? 0 : targetNodeId.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        ProcessInfoKey other = (ProcessInfoKey) obj;
        if (processType != other.processType)
            return false;
        if (sourceNodeId == null) {
            if (other.sourceNodeId != null)
                return false;
        } else if (!sourceNodeId.equals(other.sourceNodeId))
            return false;
        if (targetNodeId == null) {
            if (other.targetNodeId != null)
                return false;
        } else if (!targetNodeId.equals(other.targetNodeId))
            return false;
        return true;
    }
    
    @Override
    public String toString() {
        return String.format("processType=%s,sourceNodeId=%s,targetNodeId=%s",processType.toString(), sourceNodeId, targetNodeId);
    }
    
}
