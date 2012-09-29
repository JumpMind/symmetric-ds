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
package org.jumpmind.symmetric.fs.client.connector;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.jumpmind.symmetric.fs.config.Node;
import org.jumpmind.symmetric.fs.config.SyncConfig;
import org.jumpmind.symmetric.fs.service.IPersisterServices;

public class TransportConnectorFactory {

    protected Map<String, Class<? extends ITransportConnector>> connectorTypes;
    
    protected IPersisterServices persisterServices;

    public TransportConnectorFactory(IPersisterServices persisterServices) {
        this.persisterServices = persisterServices;
        connectorTypes = new HashMap<String, Class<? extends ITransportConnector>>();        
        connectorTypes.put("default", HttpTransportConnector.class);
        connectorTypes.put("http", HttpTransportConnector.class);
        connectorTypes.put("local", LocalTransportConnector.class);
    }

    public void addTransportConnectorType(String name, Class<ITransportConnector> clazz) {
        connectorTypes.put(name, clazz);
    }
    
    public Set<String> getTransportConnecetorTypes() {
        return connectorTypes.keySet();
    }

    public ITransportConnector createTransportConnector(SyncConfig config, Node node) {
        ITransportConnector connector = null;
        Class<? extends ITransportConnector> clazz = connectorTypes.get(config.getTransportConnectorType());
        if (clazz != null) {
            try {
                connector = clazz.newInstance();
                connector.init(config, node, persisterServices);
                return connector;
            } catch (InstantiationException e) {
                throw new UnsupportedOperationException(e);
            } catch (IllegalAccessException e) {
                throw new UnsupportedOperationException(e);
            }
        } else {
            throw new UnsupportedOperationException(String.format(
                    "Cannot locate a transport connector named ",
                    config.getTransportConnectorType()));
        }
    }
}
