package org.jumpmind.symmetric;

import java.io.File;

import javax.sql.DataSource;

import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.TestConstants;
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.db.SqlScript;
import org.jumpmind.symmetric.service.IBootstrapService;

abstract public class AbstractIntegrationTest extends AbstractTest {

    private SymmetricEngine clientEngine;

    private SymmetricEngine rootEngine;

    abstract File getClientFile();

    abstract File getRootFile();
    
    protected SymmetricEngine getClientEngine() {
        if (this.clientEngine == null) {
            this.clientEngine = createEngine(getClientFile());
            dropAndCreateDatabaseTables(getClientDatabaseName(), clientEngine);
        }
        return this.clientEngine;
    }
    
    protected String getRootDatabaseName() {
        IDbDialect dialect = (IDbDialect)getRootEngine().getApplicationContext().getBean(Constants.DB_DIALECT);
        return dialect.getName().toLowerCase();
    }

    protected String getClientDatabaseName() {
        IDbDialect dialect = (IDbDialect)getClientEngine().getApplicationContext().getBean(Constants.DB_DIALECT);
        return dialect.getName().toLowerCase();
    }


    protected SymmetricEngine getRootEngine() {
        if (this.rootEngine == null) {
            this.rootEngine = createEngine(getRootFile());
            dropAndCreateDatabaseTables(getRootDatabaseName(), rootEngine);
            ((IBootstrapService) this.rootEngine.getApplicationContext().getBean(Constants.BOOTSTRAP_SERVICE)).init();
            new SqlScript(getResource(TestConstants.TEST_ROOT_DOMAIN_SETUP_SCRIPT), (DataSource) this.rootEngine
                    .getApplicationContext().getBean(Constants.DATA_SOURCE), true).execute();
            this.rootEngine.start();
        }
        return this.rootEngine;
    }
}
