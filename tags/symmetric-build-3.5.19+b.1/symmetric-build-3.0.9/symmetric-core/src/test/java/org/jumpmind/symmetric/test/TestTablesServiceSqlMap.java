package org.jumpmind.symmetric.test;

import java.util.Map;

import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.symmetric.service.impl.AbstractSqlMap;
import org.junit.Ignore;

@Ignore
public class TestTablesServiceSqlMap extends AbstractSqlMap {

    public TestTablesServiceSqlMap(IDatabasePlatform platform, Map<String, String> replacementTokens) {
        super(platform, replacementTokens);

        putSql("insertCustomerSql",
                "insert into test_customer "
                        + "(customer_id, name, is_active, address, city, state, zip, entry_timestamp, entry_time, notes, icon) "
                        + "values(?,?,?,?,?,?,?,?,?,?,?)");

        putSql("insertIntoTestTriggersTableSql",
                "insert into test_triggers_table (id, string_one_value, string_two_value) values(?,?,?)");
        
        putSql("insertOrderSql", "insert into test_order_header (order_id, customer_id, status, deliver_date) values(?,?,?,?)");
        
        putSql("insertOrderDetailSql", "insert into test_order_detail (order_id, line_number, item_type, item_id, quantity, price) values(?,?,?,?,?,?)");
        
        putSql("selectOrderSql", "select order_id, customer_id, status, deliver_date from test_order_header where order_id = ?");
    }

}
