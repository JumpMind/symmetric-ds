package org.jumpmind.symmetric.core.service;

import org.jumpmind.symmetric.AbstractDatabaseTest;
import org.jumpmind.symmetric.core.DefaultEnvironment;
import org.jumpmind.symmetric.core.IEnvironment;
import org.jumpmind.symmetric.core.model.Parameters;
import org.jumpmind.symmetric.core.resources.DefaultResourceFactory;

public class ParameterServiceTest extends AbstractParameterServiceTest {

    protected IEnvironment environment;

    @Override
    protected IEnvironment getEnvironment() {
        if (environment == null) {
            environment = new DefaultEnvironment(new DefaultResourceFactory(),
                    AbstractDatabaseTest.getDbDialect(), new Parameters());
        }
        return environment;
    }

    @Override
    protected void resetEnvironment() {

    }
}
