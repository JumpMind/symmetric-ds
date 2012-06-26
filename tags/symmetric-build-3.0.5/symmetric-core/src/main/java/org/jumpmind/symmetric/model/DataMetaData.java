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

import org.jumpmind.db.model.Table;

public class DataMetaData {

    private Data data;
    private Table table;
    private TriggerRouter triggerRouter;
    private NodeChannel nodeChannel;

    public DataMetaData(Data data, Table table, TriggerRouter triggerRouter, NodeChannel nodeChannel) {
        this.data = data;
        this.table = table;
        this.triggerRouter = triggerRouter;
        this.nodeChannel = nodeChannel;
    }

    public Data getData() {
        return data;
    }

    public void setData(Data data) {
        this.data = data;
    }

    public Table getTable() {
        return table;
    }

    public void setTable(Table table) {
        this.table = table;
    }

    public TriggerRouter getTriggerRouter() {
        return triggerRouter;
    }

    public void setTriggerRouter(TriggerRouter trigger) {
        this.triggerRouter = trigger;
    }

    public NodeChannel getNodeChannel() {
        return nodeChannel;
    }

    public void setNodeChannel(NodeChannel nodeChannel) {
        this.nodeChannel = nodeChannel;
    }

    public TriggerHistory getTriggerHistory() {
        return data != null ? data.getTriggerHistory() : null;
    }

}