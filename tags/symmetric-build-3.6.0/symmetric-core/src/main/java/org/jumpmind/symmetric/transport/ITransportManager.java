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

package org.jumpmind.symmetric.transport;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;

import org.jumpmind.symmetric.model.BatchAck;
import org.jumpmind.symmetric.model.IncomingBatch;
import org.jumpmind.symmetric.model.Node;

public interface ITransportManager {

    public int sendAcknowledgement(Node remote, List<IncomingBatch> list, Node local, String securityToken, String registrationUrl) throws IOException;

    public void writeAcknowledgement(OutputStream out, Node remote, List<IncomingBatch> list, Node local, String securityToken) throws IOException;

    public List<BatchAck> readAcknowledgement(String parameterString1, String parameterString2) throws IOException;
    
    public IIncomingTransport getFilePullTransport(Node remote, Node local, String securityToken,
            Map<String, String> requestProperties, String registrationUrl) throws IOException;   
    
    public IOutgoingWithResponseTransport getFilePushTransport(Node remote, Node local,
            String securityToken, String registrationUrl) throws IOException;

    public IIncomingTransport getPullTransport(Node remote, Node local, String securityToken, Map<String,String> requestProperties, String registrationUrl) throws IOException;

    public IOutgoingWithResponseTransport getPushTransport(Node remote, Node local, String securityToken, String registrationUrl) throws IOException;

    public IIncomingTransport getRegisterTransport(Node node, String registrationUrl) throws IOException;
    
    public void addExtensionSyncUrlHandler(String name, ISyncUrlExtension handler);
    
    /**
     * This is the proper way to determine the URL for a node.  It delegates to configured 
     * extension points when necessary to take in to account custom load balancing and
     * url selection schemes.
     * @param url This is the url configured in sync_url of the node table
     */
    public String resolveURL(String url, String registrationUrl);

}