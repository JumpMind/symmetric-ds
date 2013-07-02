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
package org.jumpmind.symmetric.web;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

public class SymmetricContextListener implements ServletContextListener {

    public void contextInitialized(ServletContextEvent sce) {
        SymmetricEngineHolder engineHolder = new SymmetricEngineHolder();
        ServletContext ctx = sce.getServletContext();
        
        String autoStart = ctx.getInitParameter(WebConstants.INIT_PARAM_AUTO_START); 
        engineHolder.setAutoStart(autoStart == null ? true : autoStart.equalsIgnoreCase("true"));
        
        String autoCreate = ctx.getInitParameter(WebConstants.INIT_PARAM_AUTO_CREATE); 
        engineHolder.setAutoCreate(autoCreate == null ? true : autoCreate.equalsIgnoreCase("true"));
        
        String multiServerMode = ctx.getInitParameter(WebConstants.INIT_PARAM_MULTI_SERVER_MODE);
        engineHolder.setMultiServerMode(multiServerMode != null
                && multiServerMode.equalsIgnoreCase("true"));
        
        engineHolder.setSingleServerPropertiesFile(ctx
                .getInitParameter(WebConstants.INIT_SINGLE_SERVER_PROPERTIES_FILE));
        
        String staticEnginesMode = ctx.getInitParameter(WebConstants.INIT_PARAM_STATIC_ENGINES_MODE);
        engineHolder.setStaticEnginesMode(staticEnginesMode != null
                && staticEnginesMode.equalsIgnoreCase("true"));
        
        engineHolder.setDeploymentType(ctx.getInitParameter(WebConstants.INIT_PARAM_DEPLOYMENT_TYPE));
        ctx.setAttribute(WebConstants.ATTR_ENGINE_HOLDER, engineHolder);
        engineHolder.start();
    }

    public void contextDestroyed(ServletContextEvent sce) {
        ServletContext ctx = sce.getServletContext();
        SymmetricEngineHolder engineHolder = (SymmetricEngineHolder) ctx
                .getAttribute(WebConstants.ATTR_ENGINE_HOLDER);
        if (engineHolder != null) {
            engineHolder.stop();
            ctx.removeAttribute(WebConstants.ATTR_ENGINE_HOLDER);
        }
    }

}
