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
package org.jumpmind.symmetric.io.stage;

import java.io.BufferedReader;
import java.io.BufferedWriter;

public interface IStagedResource {

    public enum State {
        CREATE, READY, DONE;

        public String getExtensionName() {
            return name().toLowerCase();
        }

    };

    public BufferedReader getReader();

    public BufferedWriter getWriter();
    
    public void close();

    public long getSize();

    public State getState();
    
    public String getPath();
    
    public void setState(State state);
    
    public long getCreateTime();
    
    public boolean isFileResource();
    
    public void delete();
    
    public boolean exists();

}
