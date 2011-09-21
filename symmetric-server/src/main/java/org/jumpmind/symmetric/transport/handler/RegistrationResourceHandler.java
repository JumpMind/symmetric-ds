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

package org.jumpmind.symmetric.transport.handler;

import java.io.IOException;
import java.io.OutputStream;

import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.service.IRegistrationService;

/**
 * Handler that delegates to the {@link IRegistrationService}
 */
public class RegistrationResourceHandler extends AbstractTransportResourceHandler {
    private IRegistrationService registrationService;

    public boolean registerNode(Node node, String remoteHost, String remoteAddress, OutputStream outputStream) throws IOException {
        return getRegistrationService().registerNode(node, remoteHost, remoteAddress, outputStream, true);
    }

    private IRegistrationService getRegistrationService() {
        return registrationService;
    }

    public void setRegistrationService(IRegistrationService registrationService) {
        this.registrationService = registrationService;
    }
}