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
package org.jumpmind.symmetric.test;

import java.io.File;
import java.io.IOException;
import java.net.URL;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.DbTestUtils;
import org.jumpmind.db.model.Database;
import org.jumpmind.db.platform.AbstractJdbcDatabasePlatform;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.platform.IDdlBuilder;
import org.jumpmind.db.sql.SqlScript;
import org.jumpmind.properties.EnvironmentSpecificProperties;
import org.jumpmind.symmetric.ClientSymmetricEngine;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.common.SystemConstants;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract public class TestSetupUtil {

    private static final Logger logger = LoggerFactory.getLogger(TestSetupUtil.class);

    static private ISymmetricEngine engine;

    public static ISymmetricEngine prepareForServiceTests() {
        System.setProperty(SystemConstants.SYSPROP_WAIT_FOR_DATABASE, "false");
        if (engine == null) {
            engine = prepareRoot("/test-services-setup.sql");
            engine.start();
        }
        return engine;
    }

    protected static ISymmetricEngine prepareRoot() {
        System.setProperty(SystemConstants.SYSPROP_WAIT_FOR_DATABASE, "false");
        return prepareRoot(null);
    }

    protected static ISymmetricEngine prepareRoot(String sql) {
        removeEmbededdedDatabases();
        EnvironmentSpecificProperties properties = new EnvironmentSpecificProperties(new URL[] {
                getResource(DbTestUtils.DB_TEST_PROPERTIES),
                getResource("/symmetric-test.properties") }, "test.root", new String[] { "root" });
        if (StringUtils.isNotBlank(sql)) {
            properties.setProperty(ParameterConstants.AUTO_CONFIGURE_REG_SVR_SQL_SCRIPT, sql);
        }
        ISymmetricEngine engine = new ClientSymmetricEngine(properties);
        dropAndCreateDatabaseTables(properties.getProperty("test.root"), engine);
        return engine;
    }
    
    public static IDatabasePlatform dropDatabaseTables(String databaseType, ISymmetricEngine engine) {

        ISymmetricDialect dialect = engine.getSymmetricDialect();

        AbstractJdbcDatabasePlatform platform = (AbstractJdbcDatabasePlatform) dialect
                .getPlatform();

        engine.uninstall();       
        
        platform.resetDataSource();        
        
        IDdlBuilder builder = platform.getDdlBuilder();
        
        Database db2drop = platform.readDatabase(platform.getDefaultCatalog(),
                platform.getDefaultSchema(), new String[] { "TABLE" });
        
        platform.resetDataSource();
        
        String sql = builder.dropTables(db2drop);                
        SqlScript dropScript = new SqlScript(sql, platform.getSqlTemplate(), false, platform.getSqlScriptReplacementTokens());
        dropScript.execute(true);
        
        platform.resetDataSource();

        dialect.purgeRecycleBin();
        
        platform.resetCachedTableModel();

        return platform;
    }

    public static void dropAndCreateDatabaseTables(String databaseType, ISymmetricEngine engine) {
        IDatabasePlatform platform = dropDatabaseTables(databaseType, engine);
        Database testDb = platform.readDatabaseFromXml("/test-schema.xml", true);
        platform.createDatabase(testDb, false, true);
    }

    protected static void removeEmbededdedDatabases() {
        File clientDbDir = new File("target/clientdbs");
        if (clientDbDir.exists()) {
            try {
                logger.info("Removing client database files and creating directory.");
                FileUtils.deleteDirectory(clientDbDir);
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
            }
        }
        clientDbDir.mkdir();
        File rootDbDir = new File("target/rootdbs");
        if (rootDbDir.exists()) {
            try {
                logger.info("Removing root database files and creating directory.");
                FileUtils.deleteDirectory(rootDbDir);
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
            }
        }
        rootDbDir.mkdir();
    }

    protected static URL getResource(String resource) {
        return TestSetupUtil.class.getResource(resource);
    }

}
