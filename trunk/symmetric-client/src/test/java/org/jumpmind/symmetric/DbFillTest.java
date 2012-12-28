package org.jumpmind.symmetric;

import org.jumpmind.symmetric.io.data.DbFill;
import org.jumpmind.symmetric.service.impl.AbstractServiceTest;

public class DbFillTest extends AbstractServiceTest {

    
//    @Test
    public void dbFillTest() {
        ISymmetricEngine engine = getSymmetricEngine();
        
        DbFill dbFill = new DbFill(engine.getDatabasePlatform());
        dbFill.setRecordCount(5);
        
        dbFill.fillTables(new String[0]);
    }
    
}
