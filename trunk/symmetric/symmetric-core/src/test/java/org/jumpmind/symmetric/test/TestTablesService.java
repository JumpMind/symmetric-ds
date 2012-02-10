package org.jumpmind.symmetric.test;

import java.sql.Types;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.DatabaseNamesConstants;
import org.jumpmind.db.sql.ISqlRowMapper;
import org.jumpmind.db.sql.ISqlTransaction;
import org.jumpmind.db.sql.Row;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.service.impl.AbstractService;
import org.junit.Ignore;

@Ignore
public class TestTablesService extends AbstractService {

    public TestTablesService(ISymmetricEngine engine) {
        super(engine.getParameterService(), engine.getSymmetricDialect());
        setSqlMap(new TestTablesServiceSqlMap(platform, createSqlReplacementTokens()));
    }

    // TODO support insert of blob test for postgres and informix
    public boolean insertIntoTestUseStreamLob(int id, String tableName, String lobValue) {
        if (!DatabaseNamesConstants.POSTGRESQL.equals(platform.getName())
                && !DatabaseNamesConstants.INFORMIX.equals(platform.getName())) {
            sqlTemplate.update(
                    String.format("insert into %s (test_id, test_blob) values(?, ?)", tableName),
                    new Object[] { id, lobValue.getBytes() },
                    new int[] { Types.INTEGER, Types.BLOB });
            return true;
        } else {
            return false;
        }
    }

    // TODO support insert of blob test for postgres and informix
    public boolean updateTestUseStreamLob(int id, String tableName, String lobValue) {
        if (!DatabaseNamesConstants.POSTGRESQL.equals(platform.getName())
                && !DatabaseNamesConstants.INFORMIX.equals(platform.getName())) {
            sqlTemplate.update(
                    String.format("update %s set test_blob=? where test_id=?", tableName),
                    new Object[] { lobValue.getBytes(), id },
                    new int[] { Types.BLOB, Types.INTEGER });
            return true;
        } else {
            return false;
        }
    }

    public void assertTestUseStreamBlobInDatabase(int id, String tableName, String expected) {
        if (!DatabaseNamesConstants.POSTGRESQL.equals(platform.getName())
                && !DatabaseNamesConstants.INFORMIX.equals(platform.getName())) {
            Map<String, Object> values = sqlTemplate.queryForMap("select test_blob from "
                    + tableName + " where test_id=?", id);
            Assert.assertEquals(
                    "The blob column for test_use_stream_lob was not loaded into the client database",
                    expected, new String((byte[]) values.get("TEST_BLOB")));
        }
    }

    public void insertOrder(Order order) {
        sqlTemplate.update(getSql("insertOrderSql"), order.getOrderId(), order.getCustomerId(),
                order.getStatus(), order.getDeliverDate());
        List<OrderDetail> details = order.getOrderDetails();
        for (OrderDetail orderDetail : details) {
            sqlTemplate.update(getSql("insertOrderDetailSql"), orderDetail.getOrderId(),
                    orderDetail.getLineNumber(), orderDetail.getItemType(),
                    orderDetail.getItemId(), orderDetail.getQuantity(), orderDetail.getPrice());
        }
    }

    public Order getOrder(String id) {
        return sqlTemplate.queryForObject(getSql("selectOrderSql"), new ISqlRowMapper<Order>() {
            public Order mapRow(Row rs) {
                return new Order(rs.getString("order_id"), rs.getInt("customer_id"), rs
                        .getString("status"), rs.getDateTime("deliver_date"));
            }
        }, id);
    }

    public void insertCustomer(Customer customer) {
        sqlTemplate.update(getSql("insertCustomerSql"), customer.getCustomerId(),
                customer.getName(), customer.isActive() ? "1" : "0", customer.getAddress(),
                customer.getCity(), customer.getState(), customer.getZip(),
                customer.getEntryTimestamp(), customer.getEntryTime(), customer.getNotes(),
                customer.getIcon());
    }

    public int updateCustomer(int id, String column, Object value) {
        return sqlTemplate.update(
                String.format("update test_customer set %s=? where customer_id=?", column), value,
                id);
    }

    public Customer getCustomer(int id) {
        return sqlTemplate.queryForObject("select * from test_customer where customer_id=?",
                new ISqlRowMapper<Customer>() {
                    public Customer mapRow(Row rs) {
                        return new Customer(rs.getInt("customer_id"), rs.getString("name"), rs
                                .getBoolean("is_active"), rs.getString("address"), rs
                                .getString("city"), rs.getString("state"), rs.getInt("zip"), rs
                                .getDateTime("entry_timestamp"), rs.getDateTime("entry_time"), rs
                                .getString("notes"), rs.getBytes("icon"));
                    }
                }, id);
    }
    
    public int count(String table) {
        return sqlTemplate.queryForInt(String.format("select count(*) from %s",table));
    }

    public boolean doesCustomerExist(int id) {
        return sqlTemplate
                .queryForInt("select count(*) from test_customer where customer_id=?", id) > 0;
    }

    public String getCustomerNotes(int id) {
        return sqlTemplate
                .queryForString("select notes from test_customer where customer_id=?", id);
    }

    public byte[] getCustomerIcon(int id) {
        return sqlTemplate.queryForObject("select icon from test_customer where customer_id=?",
                new ISqlRowMapper<byte[]>() {
                    public byte[] mapRow(org.jumpmind.db.sql.Row rs) {
                        return rs.bytesValue();
                    }
                }, id);
    }

    public void insertIntoTestTriggerTable(Object[] values) {
        Table testTriggerTable = platform
                .getTableFromCache(null, null, "test_triggers_table", true);
        ISqlTransaction transaction = sqlTemplate.startSqlTransaction();
        try {
            transaction.allowInsertIntoAutoIncrementColumns(true, testTriggerTable);
            transaction.prepareAndExecute(getSql("insertIntoTestTriggersTableSql"), values);
            transaction.commit();
        } finally {
            transaction.allowInsertIntoAutoIncrementColumns(false, testTriggerTable);
            transaction.close();
        }
    }

    public int countTestTriggersTable() {
        return sqlTemplate.queryForInt("select count(*) from test_triggers_table");
    }

}
