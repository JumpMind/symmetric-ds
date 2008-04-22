package org.jumpmind.symmetric;

import java.io.File;
import java.util.Properties;

import javax.sql.DataSource;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.symmetric.MultiDatabaseTest.DatabaseRole;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.TestConstants;
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.db.SqlScript;
import org.jumpmind.symmetric.service.IBootstrapService;

abstract public class AbstractIntegrationTest extends AbstractTest {

    private SymmetricEngine clientEngine;

    private SymmetricEngine rootEngine;

    protected SymmetricEngine getClientEngine() {
        if (this.clientEngine == null) {
            this.clientEngine = createEngine(getClientFile());
            dropAndCreateDatabaseTables(getClientDatabaseName(), clientEngine);
        }
        return this.clientEngine;
    }
    
    protected String getRootDatabaseName() {
        return getRootDbDialect().getName().toLowerCase();
    }

    protected String getClientDatabaseName() {
        return getClientDbDialect().getName().toLowerCase();
    }

    protected SymmetricEngine getRootEngine() {
        if (this.rootEngine == null) {
            this.rootEngine = createEngine(getRootFile());
            dropAndCreateDatabaseTables(getRootDatabaseName(), rootEngine);
            ((IBootstrapService) this.rootEngine.getApplicationContext().getBean(Constants.BOOTSTRAP_SERVICE)).setupDatabase();
            new SqlScript(getResource(TestConstants.TEST_ROOT_DOMAIN_SETUP_SCRIPT), (DataSource) this.rootEngine
                    .getApplicationContext().getBean(Constants.DATA_SOURCE), true).execute();
            this.rootEngine.start();
        }
        return this.rootEngine;
    }
    
    protected IDbDialect getRootDbDialect() {
        return (IDbDialect)getRootEngine().getApplicationContext().getBean(Constants.DB_DIALECT);
    }
    
    protected IDbDialect getClientDbDialect() {
        return (IDbDialect)getClientEngine().getApplicationContext().getBean(Constants.DB_DIALECT);
    }
    

    File getClientFile() {
        Properties properties = MultiDatabaseTest.getTestProperties();
        String[] databaseTypes = StringUtils.split(properties.getProperty("test.client"), ",");
        return MultiDatabaseTest.writeTempPropertiesFileFor(databaseTypes[0], DatabaseRole.CLIENT);
    }

    File getRootFile() {
        Properties properties = MultiDatabaseTest.getTestProperties();
        String[] databaseTypes = StringUtils.split(properties.getProperty("test.root"), ",");

        return MultiDatabaseTest.writeTempPropertiesFileFor(databaseTypes[0], DatabaseRole.ROOT);
    }

}
