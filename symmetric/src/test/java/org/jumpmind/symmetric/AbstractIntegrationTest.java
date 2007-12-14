package org.jumpmind.symmetric;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;

import javax.sql.DataSource;

import org.apache.ddlutils.Platform;
import org.apache.ddlutils.io.DatabaseIO;
import org.apache.ddlutils.model.Database;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.TestConstants;
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.db.SqlScript;
import org.jumpmind.symmetric.db.oracle.OracleDbDialect;
import org.jumpmind.symmetric.service.IBootstrapService;

abstract public class AbstractIntegrationTest {

    private SymmetricEngine clientEngine;

    private SymmetricEngine rootEngine;

    abstract File getClientFile();

    abstract File getRootFile();

    protected SymmetricEngine getClientEngine() {
        if (this.clientEngine == null) {
            this.clientEngine = createEngine(getClientFile());
            dropAndCreateDatabaseTables(clientEngine);
        }
        return this.clientEngine;
    }

    protected SymmetricEngine getRootEngine() {
        if (this.rootEngine == null) {
            this.rootEngine = createEngine(getRootFile());
            dropAndCreateDatabaseTables(rootEngine);
            ((IBootstrapService) this.rootEngine.getApplicationContext().getBean(Constants.BOOTSTRAP_SERVICE)).init();
            new SqlScript(getResource(TestConstants.TEST_ROOT_DOMAIN_SETUP_SCRIPT), (DataSource) this.rootEngine
                    .getApplicationContext().getBean(Constants.DATA_SOURCE), true).execute();
            this.rootEngine.start();
        }
        return this.rootEngine;
    }

    private SymmetricEngine createEngine(File propertiesFile) {
        return new SymmetricEngine("file:" + propertiesFile.getAbsolutePath(), null);
    }

    private void dropAndCreateDatabaseTables(SymmetricEngine engine) {
        DataSource ds = (DataSource) engine.getApplicationContext().getBean(Constants.DATA_SOURCE);
        try {
            IDbDialect dialect = (IDbDialect) engine.getApplicationContext().getBean(Constants.DB_DIALECT);
            Platform platform = dialect.getPlatform();
            Database testDb = getTestDatabase();
            platform.dropTables(testDb, true);
            dialect.purge();

            new SqlScript(getResource(TestConstants.TEST_DROP_ALL_SCRIPT), ds, false).execute();

            // might want to come up with a slicker way to do db specific drop/create
            if (dialect instanceof OracleDbDialect) {
                new SqlScript(getResource(TestConstants.TEST_DROP_SEQ_SCRIPT), ds, false).execute();
            }

            platform.createTables(testDb, false, true);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Database getTestDatabase() throws IOException {
        return new DatabaseIO().read(new InputStreamReader(getResource("/test-tables-ddl.xml").openStream()));
    }

    private URL getResource(String resource) {
        return SymmetricEngineTestFactory.class.getResource(resource);
    }

}
