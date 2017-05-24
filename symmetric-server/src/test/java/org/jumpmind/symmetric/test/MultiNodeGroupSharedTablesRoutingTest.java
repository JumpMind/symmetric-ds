package org.jumpmind.symmetric.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.sql.Types;
import java.util.List;
import java.util.UUID;

import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Table;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.model.OutgoingBatch;

public class MultiNodeGroupSharedTablesRoutingTest extends AbstractTest {

    public MultiNodeGroupSharedTablesRoutingTest() {
    }
    
    @Override
    protected String[] getGroupNames() {
        return new String[] { "server", "client", "client2"};
    }
    
    @Override
    protected Table[] getTables(String name) {
        Table tableA = new Table("A");
        tableA.addColumn(new Column("ID", true, Types.INTEGER, 100, 0));
        tableA.addColumn(new Column("NOTE", false, Types.VARCHAR, 100, 0));
        
        Table tableB = new Table("B");
        tableB.addColumn(new Column("ID", true, Types.INTEGER, 100, 0));
        tableB.addColumn(new Column("NOTE", false, Types.VARCHAR, 100, 0));
        
        Table tableC = new Table("C");
        tableC.addColumn(new Column("ID", true, Types.INTEGER, 100, 0));
        tableC.addColumn(new Column("NOTE", false, Types.VARCHAR, 100, 0));
        return new Table[] { tableA, tableB, tableC };
    }

    @Override
    protected void test(ISymmetricEngine rootServer, ISymmetricEngine clientServer) throws Exception {
        loadConfigAtRegistrationServer();
        
        rootServer.openRegistration("client", "client");
        pull("client");

        rootServer.openRegistration("client2", "client2");
        pull("client2");
        
        for (int i = 1; i < 11; i++) {            
            template(rootServer).update("insert into c values(?,?)", i, UUID.randomUUID().toString());
            template(rootServer).update("insert into b values(?,?)", i, UUID.randomUUID().toString());
            template(rootServer).update("insert into a values(?,?)", i, UUID.randomUUID().toString());
        }
        
        rootServer.route();
        
        List<OutgoingBatch> batches1 = rootServer.getOutgoingBatchService().getOutgoingBatches("client", false).filterBatchesForChannel("default");
        List<OutgoingBatch> batches2 = rootServer.getOutgoingBatchService().getOutgoingBatches("client2", false).filterBatchesForChannel("default");
        
        assertEquals(1, batches1.size());
        assertEquals(1, batches2.size());
        assertEquals(30, batches1.get(0).getDataRowCount());
        assertEquals(30, batches1.get(0).getDataInsertRowCount());
        assertEquals(10, batches2.get(0).getDataRowCount());
        assertEquals(10, batches2.get(0).getDataInsertRowCount());
        
        assertEquals(30, template(rootServer).queryForInt("select count(*) from sym_data_event where batch_id=?", batches1.get(0).getBatchId()));
        assertEquals(10, template(rootServer).queryForInt("select count(*) from sym_data_event where batch_id=?", batches2.get(0).getBatchId()));
        
        assertNotEquals(batches1.get(0), batches2.get(0));
        
    }

}
