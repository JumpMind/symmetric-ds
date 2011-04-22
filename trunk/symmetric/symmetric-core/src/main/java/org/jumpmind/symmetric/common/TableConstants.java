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

package org.jumpmind.symmetric.common;

import java.util.HashSet;
import java.util.Set;

/**
 * Constants that represent SymmetricDS tables
 */
public class TableConstants {

    public static final String SYM_TRIGGER = "trigger";
    public static final String SYM_TRIGGER_ROUTER = "trigger_router";
    public static final String SYM_ROUTER = "router";
    public static final String SYM_TRIGGER_HIST = "trigger_hist";
    public static final String SYM_CHANNEL = "channel";
    public static final String SYM_NODE_GROUP = "node_group";
    public static final String SYM_NODE_GROUP_LINK = "node_group_link";

    public static final String SYM_NODE = "node";
    public static final String SYM_NODE_HOST = "node_host";
    public static final String SYM_NODE_SECURITY = "node_security";
    public static final String SYM_NODE_IDENTITY = "node_identity";
    public static final String SYM_NODE_CHANNEL_CTL = "node_channel_ctl";
    public static final String SYM_PARAMETER = "parameter";    

    public static String[] NODE_TABLES = { SYM_NODE, SYM_NODE_SECURITY, SYM_NODE_IDENTITY };

    public static final Set<String> getNodeTablesAsSet(String tablePrefix) {
        Set<String> tables = new HashSet<String>();
        tables.add(getTableName(tablePrefix, SYM_NODE));
        tables.add(getTableName(tablePrefix, SYM_NODE_SECURITY));
        tables.add(getTableName(tablePrefix, SYM_NODE_IDENTITY));
        tables.add(getTableName(tablePrefix, SYM_NODE_CHANNEL_CTL));
        return tables;
    }

    public static String getTableName(String tablePrefix, String tableSuffix) {
        return String.format("%s_%s", tablePrefix, tableSuffix);
    }
}