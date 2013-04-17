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


package org.jumpmind.symmetric;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;

import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.logging.ILog;
import org.jumpmind.symmetric.common.logging.LogFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.web.context.ConfigurableWebApplicationContext;
import org.springframework.web.context.ContextLoader;
import org.springframework.web.context.ContextLoaderListener;
import org.springframework.web.context.support.WebApplicationContextUtils;

/**
 * This is the standard way to bootstrap SymmetricDS in a stand-alone web container.
 * SymmetricDS uses Spring's WebApplicationContext for access to SymmetricDS from
 * its Servlets. This Servlet context listener forces the contextConfigLocation
 * for Spring to be load symmetric.xml.
 * <p/>
 * Developers have the option to subclass off of this listener and override the
 * createConfigureAndStartEngine() method.
 * 
 * @deprecated
 *
 * 
 */
public class SymmetricEngineContextLoaderListener extends ContextLoaderListener {

    static final String SYMMETRIC_SPRING_LOCATION = "classpath:/symmetric.xml";
    static final String SYMMETRIC_EMPTY_SPRING_LOCATION = "classpath:/symmetric-empty.xml";

    static final ILog log = LogFactory.getLog(SymmetricEngineContextLoaderListener.class);

    ISymmetricEngine engine = null;

    public SymmetricEngineContextLoaderListener() {
    }

    public SymmetricEngineContextLoaderListener(ISymmetricEngine engine) {
        this.engine = engine;
    }

    @Override
    final public void contextInitialized(ServletContextEvent event) {
        try {
            super.contextInitialized(event);
            createConfigureAndStartEngine(WebApplicationContextUtils.getWebApplicationContext(event.getServletContext()));
        } catch (Exception ex) {
            log.error("WebServerInitializeError", ex);
            if (ex instanceof RuntimeException) {
                throw (RuntimeException) ex;
            } else {
                throw new RuntimeException(ex);
            }
        }
    }

    @Override
    public void contextDestroyed(ServletContextEvent event) {
        if (engine != null) {
            engine.stop();
            engine = null;
        }
    }

    protected void createConfigureAndStartEngine(ApplicationContext ctx) {
        if (this.engine == null) {
            this.engine = new StandaloneSymmetricEngine(ctx, ctx.containsBean(Constants.PROPERTIES) ? false : true);
        }
        engine.start();
    }

    @Override
    protected ContextLoader createContextLoader() {
        return new ContextLoader() {
            @Override
            protected void customizeContext(ServletContext servletContext,
                    ConfigurableWebApplicationContext applicationContext) {
                if (engine == null) {
                    String[] configLocation = applicationContext.getConfigLocations();
                    String[] newconfigLocation = new String[configLocation.length + 1];
                    newconfigLocation[0] = SYMMETRIC_SPRING_LOCATION;
                    boolean symmetricConfigured = false;
                    for (int i = 0; i < configLocation.length; i++) {
                        String config = configLocation[i];
                        if (config.equals(SYMMETRIC_SPRING_LOCATION)) {
                            symmetricConfigured = true;
                        }
                        newconfigLocation[i + 1] = configLocation[i];
                    }

                    if (!symmetricConfigured) {
                        applicationContext.setConfigLocations(newconfigLocation);
                    }
                } else {
                    applicationContext.setParent(engine.getApplicationContext());
                    applicationContext.setConfigLocation(SYMMETRIC_EMPTY_SPRING_LOCATION);

                }

            }
        };
    }

    public ISymmetricEngine getEngine() {
        return engine;
    }

}