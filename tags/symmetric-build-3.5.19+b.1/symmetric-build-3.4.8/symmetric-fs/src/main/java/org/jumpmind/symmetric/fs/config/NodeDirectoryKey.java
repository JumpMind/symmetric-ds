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
package org.jumpmind.symmetric.fs.config;

public class NodeDirectoryKey {

    protected String key;

    public NodeDirectoryKey(Node node, String directory) {
        this.key = String.format(
                "%s_%s_%s",
                node.getNodeId(),
                node.getGroupId(),
                Integer.toHexString(directory
                        .replace(System.getProperty("file.separator").charAt(0), '_').hashCode()));
    }

    @Override
    public String toString() {
        return key;
    }
}
