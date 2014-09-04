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
package org.jumpmind.db.model;

import java.io.Serializable;

public class PlatformColumn implements Serializable, Cloneable {

    private static final long serialVersionUID = 1L;
    
    private String name;
    
    private String type;
    
    private int size = -1;
    
    private int decimalDigits = -1;
    
    public PlatformColumn(String name, String type, int size, int decimalDigits) {
        this.name = name;
        this.type = type;
        this.size = size;
        this.decimalDigits = decimalDigits;
    }
    
    public PlatformColumn() {
    }
    
    public void setName(String name) {
        this.name = name;
    }
    
    public String getName() {
        return name;
    }
    
    public void setSize(int size) {
        this.size = size;
    }
    
    public int getSize() {
        return size;
    }
    
    public void setType(String type) {
        this.type = type;
    }
    
    public String getType() {
        return type;
    }
    
    public void setDecimalDigits(int decimalDigits) {
        this.decimalDigits = decimalDigits;
    }
    
    public int getDecimalDigits() {
        return decimalDigits;
    }    
        
    @Override
    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

}
