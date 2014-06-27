package org.jumpmind.db.io;

import junit.framework.Assert;

import org.jumpmind.db.model.Database;
import org.jumpmind.db.model.Table;
import org.junit.Test;

public class DatabaseIOTest {
    
    @Test
    public void testReadXml() {
        Database database = DatabaseXmlUtil.read(getClass().getResourceAsStream("/testDatabaseIO.xml"));
        Assert.assertNotNull(database);
        Assert.assertEquals(1, database.getTableCount());
        Assert.assertEquals("test", database.getName());
        
        Table table = database.getTable(0);
        Assert.assertEquals("test_simple_table", table.getName());
        Assert.assertEquals(8, table.getColumnCount());
        Assert.assertEquals(1, table.getPrimaryKeyColumnCount());
        Assert.assertEquals("id", table.getPrimaryKeyColumnNames()[0]);
               
    }

}
