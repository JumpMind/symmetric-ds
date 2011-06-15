package org.jumpmind.symmetric.core.service;

import org.jumpmind.symmetric.core.IEnvironment;
import org.jumpmind.symmetric.core.SymmetricClient;
import org.junit.Before;

abstract public class AbstractServiceTest {

    abstract protected IEnvironment getEnvironment();
    
    abstract protected void resetEnvironment();

    protected static SymmetricClient client;

    @Before
    public void setup() {
        IEnvironment environment = getEnvironment();
        if (client == null) {
            resetEnvironment();
            client = new SymmetricClient(environment);
            client.initialize();
        }
        
    }

}
