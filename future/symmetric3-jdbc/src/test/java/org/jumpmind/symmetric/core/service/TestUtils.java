package org.jumpmind.symmetric.core.service;

import org.jumpmind.symmetric.AbstractDatabaseTest;
import org.jumpmind.symmetric.core.DefaultEnvironment;
import org.jumpmind.symmetric.core.IEnvironment;
import org.jumpmind.symmetric.core.model.Parameters;
import org.jumpmind.symmetric.core.resources.DefaultResourceFactory;
import org.junit.Ignore;

@Ignore
public class TestUtils {

    static IEnvironment environment;
    
    public static IEnvironment getEnvironment() {
        if (environment == null) {
            environment = new DefaultEnvironment(new DefaultResourceFactory(),
                    AbstractDatabaseTest.getDbDialect(), new Parameters());
        }
        return environment;
    }
}
