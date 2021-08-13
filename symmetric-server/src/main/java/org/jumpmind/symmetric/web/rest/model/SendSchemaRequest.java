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
package org.jumpmind.symmetric.web.rest.model;

import java.util.List;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class SendSchemaRequest {
    protected String nodeGroupIdToSendTo;
    protected List<String> nodeIdsToSendTo;
    protected List<TableName> tablesToSend;

    public SendSchemaRequest() {
    }

    public void setNodeIdsToSendTo(List<String> nodeIds) {
        this.nodeIdsToSendTo = nodeIds;
    }

    public List<String> getNodeIdsToSendTo() {
        return nodeIdsToSendTo;
    }

    public void setTablesToSend(List<TableName> tableNames) {
        this.tablesToSend = tableNames;
    }

    public List<TableName> getTablesToSend() {
        return tablesToSend;
    }

    public void setNodeGroupIdToSendTo(String nodeGroupIdToSendTo) {
        this.nodeGroupIdToSendTo = nodeGroupIdToSendTo;
    }

    public String getNodeGroupIdToSendTo() {
        return nodeGroupIdToSendTo;
    }
}
