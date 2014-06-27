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
package org.jumpmind.symmetric.profile;

import java.io.Serializable;

import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.ext.IExtensionPoint;

/**
 * This extension point is for the purpose of provided SymmetricDS profiles that
 * are specific to individual applications.
 * <p/>
 * A profile might contain the configuration for a specific POS or for a specific ERP.
 */
public interface IProfile extends Serializable, IExtensionPoint {

    public String getName();
    public String getDescription();
    public void configure(ISymmetricEngine engine) throws Exception;
    public boolean isCompatible(ISymmetricEngine engine);    

}
