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
package org.jumpmind.vaadin.ui.sqlexplorer;

import java.util.ArrayList;
import java.util.List;

import javax.servlet.annotation.WebServlet;
import javax.websocket.server.ServerContainer;

import org.apache.commons.dbcp2.BasicDataSource;
import org.atmosphere.container.JSR356AsyncSupport;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.webapp.Configuration;
import org.eclipse.jetty.webapp.Configuration.ClassList;
import org.eclipse.jetty.webapp.WebAppContext;
import org.eclipse.jetty.websocket.jsr356.server.deploy.WebSocketServerContainerInitializer;
import org.h2.Driver;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.platform.JdbcDatabasePlatformFactory;
import org.jumpmind.db.sql.SqlTemplateSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vaadin.annotations.PreserveOnRefresh;
import com.vaadin.annotations.Push;
import com.vaadin.annotations.Theme;
import com.vaadin.annotations.Title;
import com.vaadin.annotations.VaadinServletConfiguration;
import com.vaadin.server.VaadinRequest;
import com.vaadin.server.VaadinServlet;
import com.vaadin.shared.communication.PushMode;
import com.vaadin.ui.UI;

@Title("SQL Explorer Demo")
@Theme("sqlexplorer")
@PreserveOnRefresh
@Push(value = PushMode.AUTOMATIC)
public class DemoUI extends UI implements IDbProvider {
    private static final Logger log = LoggerFactory.getLogger(DemoUI.class);
    private static final long serialVersionUID = 1L;
    List<IDb> dbList;

    @Override
    protected void init(VaadinRequest request) {
        dbList = new ArrayList<IDb>();
        dbList.add(new DB("DATABASE1"));
        dbList.add(new DB("DATABASE2"));
        SqlExplorer explorer = new SqlExplorer("build", this, "admin", 300);
        setContent(explorer);
        explorer.refresh();
    }

    @Override
    public List<IDb> getDatabases() {
        return dbList;
    }

    @WebServlet(urlPatterns = "/*")
    @VaadinServletConfiguration(ui = DemoUI.class, productionMode = false, widgetset = "AppWidgetset")
    public static class DemoUIServlet extends VaadinServlet {
        private static final long serialVersionUID = 1L;
    }

    public static class DB implements IDb {
        private static final long serialVersionUID = 1L;
        String name;
        IDatabasePlatform databasePlatform;

        public DB(String name) {
            this.name = name;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public IDatabasePlatform getPlatform() {
            if (databasePlatform == null) {
                BasicDataSource ds = new BasicDataSource();
                ds.setDriverClassName(Driver.class.getName());
                ds.setUrl("jdbc:h2:mem:" + name);
                databasePlatform = JdbcDatabasePlatformFactory.createNewPlatformInstance(ds, new SqlTemplateSettings(), true, false);
                StringBuilder bigTable = new StringBuilder("create table big (id integer");
                for (int i = 0; i < 300; i++) {
                    bigTable.append(",col").append(Integer.toString(i)).append(" text");
                }
                bigTable.append(")");
                databasePlatform.getSqlTemplate().update(bigTable.toString());
            }
            return databasePlatform;
        }
    }

    public static void main(String[] args) throws Exception {
        Server server = new Server(9090);
        ClassList classlist = Configuration.ClassList.setServerDefault(server);
        classlist.addBefore("org.eclipse.jetty.webapp.JettyWebXmlConfiguration", "org.eclipse.jetty.annotations.AnnotationConfiguration");
        WebAppContext webapp = new WebAppContext();
        webapp.setParentLoaderPriority(true);
        webapp.setConfigurationDiscovered(true);
        webapp.setContextPath("/");
        webapp.setResourceBase("src/main/webapp");
        webapp.setWar("src/main/webapp");
        ServletHolder servletHolder = webapp.addServlet(DemoUIServlet.class, "/*");
        servletHolder.setAsyncSupported(true);
        servletHolder.setInitParameter("org.atmosphere.cpr.asyncSupport", JSR356AsyncSupport.class.getName());
        server.setHandler(webapp);
        ServerContainer webSocketServer = WebSocketServerContainerInitializer.initialize(webapp);
        webSocketServer.setDefaultMaxSessionIdleTimeout(10000000);
        server.start();
        log.info("Browse http://localhost:9090 to see the demo");
        server.join();
    }
}
