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
import java.util.List;
import java.util.Properties;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.jumpmind.symmetric.common.InfoConstants;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.ext.IBuiltInExtensionPoint;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeGroup;
import org.jumpmind.symmetric.service.IConfigurationService;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IParameterService;

/**
 * Responsible for providing high level information about the node in property format
 */
public class InfoServlet extends AbstractResourceServlet 
  implements IBuiltInExtensionPoint {

    private static final long serialVersionUID = 1L;
    
    private INodeService nodeService;
    
    private IConfigurationService configurationService;
    
    private IParameterService parameterService;
    
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException,
            IOException {
        resp.setContentType("text/plain");
        Node node = nodeService.findIdentity();
        List<NodeGroup> nodeGroups = configurationService.getNodeGroups();
        Properties properties = new Properties();
        properties.setProperty(ParameterConstants.EXTERNAL_ID, parameterService.getExternalId());
        properties.setProperty(ParameterConstants.NODE_GROUP_ID, parameterService.getNodeGroupId());
        properties.setProperty(ParameterConstants.EXTERNAL_ID, parameterService.getExternalId());
        if (nodeGroups != null) {
            StringBuilder b = new StringBuilder();
            for (NodeGroup nodeGroup : nodeGroups) {
                b.append(nodeGroup.getNodeGroupId());
                b.append(",");
            }
            properties.setProperty(InfoConstants.NODE_GROUP_IDS, b.substring(0, b.length()-1));
        }
        
        if (node != null) {
            properties.setProperty(InfoConstants.NODE_ID, node.getNodeId());
            properties.setProperty(InfoConstants.DATABASE_TYPE, node.getDatabaseType());
            properties.setProperty(InfoConstants.DATABASE_VERSION, node.getDatabaseVersion());
            properties.setProperty(InfoConstants.DEPLOYMENT_TYPE, node.getDeploymentType());
            properties.setProperty(InfoConstants.TIMEZONE_OFFSET, node.getTimezoneOffset());
            properties.setProperty(InfoConstants.SYMMETRIC_VERSION, node.getSymmetricVersion());
        }
        
        properties.store(resp.getOutputStream(), "SymmetricDS");
        resp.flushBuffer();
    }
    
    public void setNodeService(INodeService nodeService) {
        this.nodeService = nodeService;
    }
    
    public void setConfigurationService(IConfigurationService configurationService) {
        this.configurationService = configurationService;
    }

    public void setParameterService(IParameterService parameterService) {
        this.parameterService = parameterService;
    }
}