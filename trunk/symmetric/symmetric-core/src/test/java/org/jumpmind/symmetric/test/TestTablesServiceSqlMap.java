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
    }

}
