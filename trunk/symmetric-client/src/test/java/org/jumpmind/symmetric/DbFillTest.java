package org.jumpmind.symmetric;

import javax.sql.DataSource;

import org.jumpmind.symmetric.service.impl.AbstractServiceTest;
import org.junit.Test;

public class DbFillTest extends AbstractServiceTest {

    
//    @Test
    public void dbFillTest() {

        ISymmetricEngine engine = getSymmetricEngine();
        DataSource ds = engine.getDataSource();
        
        DbFill dbFill = new DbFill(ds);
        dbFill.setRecordCount(5);
        
        dbFill.fillTables(new String[0]);
    }
    
}
