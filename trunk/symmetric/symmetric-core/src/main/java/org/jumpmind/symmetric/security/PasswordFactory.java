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


package org.jumpmind.symmetric.security;

import org.jumpmind.symmetric.common.SecurityConstants;
import org.jumpmind.symmetric.service.ISecurityService;
import org.springframework.beans.factory.FactoryBean;

/**
 * Used to protect database user and password from casual observation in the properties file
 */
public class PasswordFactory implements FactoryBean<String> {

    private ISecurityService securityService;

    private String password;

    public String getObject() throws Exception {
        if (password != null && password.startsWith(SecurityConstants.PREFIX_ENC)) {
            return securityService.decrypt(password.substring(SecurityConstants.PREFIX_ENC.length()));
        }
        return password;
    }

    public Class<String> getObjectType() {
        return String.class;
    }

    public boolean isSingleton() {
        return false;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public void setSecurityService(ISecurityService securityService) {
        this.securityService = securityService;
    }
}