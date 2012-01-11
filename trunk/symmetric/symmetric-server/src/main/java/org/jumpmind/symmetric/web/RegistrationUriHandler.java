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

package org.jumpmind.symmetric.web;

import java.io.IOException;
import java.io.OutputStream;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.log.Log;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.service.IRegistrationService;
import org.jumpmind.symmetric.service.RegistrationRedirectException;
import org.jumpmind.symmetric.transport.http.HttpTransportManager;

/**
 * Handler that delegates to the {@link IRegistrationService}
 */
public class RegistrationUriHandler extends AbstractUriHandler {
    
    private IRegistrationService registrationService;
    
    
    
    public RegistrationUriHandler(Log log,  IParameterService parameterService,
            IRegistrationService registrationService, IInterceptor... interceptors) {
        super(log, "/registration/*", parameterService, interceptors);
        this.registrationService = registrationService;
    }

    public void handle(HttpServletRequest req, HttpServletResponse res) throws IOException,
            ServletException {
        Node node = transform(req);
        try {
            OutputStream outputStream = res.getOutputStream();
            if (!registerNode(node, req.getRemoteHost(), req.getRemoteAddr(), outputStream)) {
                log.warn("RegistrationNotAllowed", node);
                ServletUtils.sendError(res, WebConstants.REGISTRATION_NOT_OPEN, String.format("%s was not allowed to register.",
                        node));
            }
        } catch (RegistrationRedirectException e) {
            res.sendRedirect(HttpTransportManager.buildRegistrationUrl(e.getRedirectionUrl(), node));
        }
    }

    private Node transform(HttpServletRequest req) {
        Node node = new Node();
        node.setNodeGroupId(ServletUtils.getParameter(req, WebConstants.NODE_GROUP_ID));
        node.setSymmetricVersion(ServletUtils.getParameter(req, WebConstants.SYMMETRIC_VERSION));
        node.setExternalId(ServletUtils.getParameter(req, WebConstants.EXTERNAL_ID));
        String syncUrlString = ServletUtils.getParameter(req, WebConstants.SYNC_URL);
        if (StringUtils.isNotBlank(syncUrlString)) {
            node.setSyncUrl(syncUrlString);
        }
        node.setSchemaVersion(ServletUtils.getParameter(req, WebConstants.SCHEMA_VERSION));
        node.setDatabaseType(ServletUtils.getParameter(req, WebConstants.DATABASE_TYPE));
        node.setDatabaseVersion(ServletUtils.getParameter(req, WebConstants.DATABASE_VERSION));
        return node;
    }

    protected boolean registerNode(Node node, String remoteHost, String remoteAddress, OutputStream outputStream) throws IOException {
        return registrationService.registerNode(node, remoteHost, remoteAddress, outputStream, true);
    }
    
}