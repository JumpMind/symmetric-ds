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
package org.jumpmind.symmetric.common;

public class DeploymentType {

    public static final String CUSTOM = "custom";
    public static final String ENGINE = "engine";
    public static final String WAR = "war";
    public static final String STANDALONE = "standalone";
    public static final String MOBILE = "mobile";
    public static final String PROFESSIONAL = "professional";
    
    private boolean engineRegistered;
    private boolean servletRegistered;
    private boolean professionalRegistered;
    private boolean mobileRegistered;
    private boolean webServerRegistered;
    
    public DeploymentType() {
        webServerRegistered = System.getProperty(Constants.PROP_STANDALONE_WEB, "false").equals("true");
        System.getProperties().remove(Constants.PROP_STANDALONE_WEB);
    }

    public void setEngineRegistered(boolean engineRegistered) {
        this.engineRegistered = engineRegistered;
    }

    public void setMobileRegistered(boolean mobileRegistered) {
        this.mobileRegistered = mobileRegistered;
    }

    public void setProfessionalRegistered(boolean professionalRegistered) {
        this.professionalRegistered = professionalRegistered;
    }

    public void setServletRegistered(boolean servletRegistered) {
        this.servletRegistered = servletRegistered;
    }

    public void setWebServerRegistered(boolean webserverRegistered) {
        this.webServerRegistered = webserverRegistered;
    }

    public String getDeploymentType() {
        if (professionalRegistered) {
            return PROFESSIONAL;
        } else if (mobileRegistered) {
            return MOBILE;
        } else if (servletRegistered && webServerRegistered) {
            return STANDALONE;
        } else if (servletRegistered && !webServerRegistered) {
            return WAR;
        } else if (engineRegistered && !servletRegistered) {
            return ENGINE;
        } else {
            return CUSTOM;
        }
    }
}
