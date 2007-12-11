package org.jumpmind.symmetric;

import org.testng.annotations.Test;

public class DatabaseTest implements ITestable {

    SymmetricEngine clientEngine;

    SymmetricEngine rootEngine;

    @Test
    public void testIt() throws Exception {

    }

    public void setClientEngine(SymmetricEngine engine) {
        this.clientEngine = engine;
    }

    public void setRootEngine(SymmetricEngine engine) {
        this.rootEngine = engine;
    }
}
