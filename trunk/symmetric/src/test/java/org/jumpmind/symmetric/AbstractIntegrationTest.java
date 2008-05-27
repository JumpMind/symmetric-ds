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

    private String rootDatabaseType;

    private String clientDatabaseType;

    AbstractIntegrationTest(String clientDatabaseType, String rootDatabaseType) {
        this.rootDatabaseType = rootDatabaseType;
        this.clientDatabaseType = clientDatabaseType;
    }

    protected SymmetricEngine getClientEngine() {
        if (this.clientEngine == null) {
            this.clientEngine = createEngine(getClientFile());
            dropAndCreateDatabaseTables(getClientDatabaseName(), clientEngine);
        }
        return this.clientEngine;
    }

    protected String getRootDatabaseName() {
        if (rootDatabaseType == null) {
            Properties properties = MultiDatabaseTest.getTestProperties();
            String[] databaseTypes = StringUtils.split(properties.getProperty("test.root"), ",");
            rootDatabaseType = databaseTypes[0];
        }        
        return rootDatabaseType;
    }

    protected String getClientDatabaseName() {
        if (clientDatabaseType == null) {
            Properties properties = MultiDatabaseTest.getTestProperties();
            String[] databaseTypes = StringUtils.split(properties.getProperty("test.client"), ",");
            clientDatabaseType = databaseTypes[0];
        }        
        return clientDatabaseType;
    }

    protected SymmetricEngine getRootEngine() {
        if (this.rootEngine == null) {
            this.rootEngine = createEngine(getRootFile());
            dropAndCreateDatabaseTables(getRootDatabaseName(), rootEngine);
            ((IBootstrapService) this.rootEngine.getApplicationContext().getBean(Constants.BOOTSTRAP_SERVICE))
                    .setupDatabase();
            new SqlScript(getResource(TestConstants.TEST_ROOT_DOMAIN_SETUP_SCRIPT),
                    (DataSource) this.rootEngine.getApplicationContext().getBean(Constants.DATA_SOURCE), true)
                    .execute();
            this.rootEngine.start();
        }
        return this.rootEngine;
    }

    protected IDbDialect getRootDbDialect() {
        return (IDbDialect) getRootEngine().getApplicationContext().getBean(Constants.DB_DIALECT);
    }

    protected IDbDialect getClientDbDialect() {
        return (IDbDialect) getClientEngine().getApplicationContext().getBean(Constants.DB_DIALECT);
    }

    File getClientFile() {
        return MultiDatabaseTest.writeTempPropertiesFileFor(getClientDatabaseName(), DatabaseRole.CLIENT);
    }

    File getRootFile() {
        return MultiDatabaseTest.writeTempPropertiesFileFor(getRootDatabaseName(), DatabaseRole.ROOT);
    }

}
