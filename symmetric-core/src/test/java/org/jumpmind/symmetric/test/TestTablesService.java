/**
 * Licensed to JumpMind Inc under one or more contributor
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU General Public License, version 3.0 (GPLv3)
 * (the "License"); you may not use this file except in compliance
 * with the License.
 *
 * You should have received a copy of the GNU General Public License,
 * version 3.0 (GPLv3) along with this library; if not, see
 * <http://www.gnu.org/licenses/>.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.jumpmind.symmetric.test;

import java.sql.Types;
import java.util.List;
import java.util.Map;

import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.DatabaseInfo;
import org.jumpmind.db.platform.DatabaseNamesConstants;
import org.jumpmind.db.sql.ISqlRowMapper;
import org.jumpmind.db.sql.ISqlTransaction;
import org.jumpmind.db.sql.Row;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.service.impl.AbstractService;
import org.junit.Assert;
import org.junit.Ignore;

@Ignore
public class TestTablesService extends AbstractService {
    public TestTablesService(ISymmetricEngine engine) {
        super(engine.getParameterService(), engine.getSymmetricDialect());
        setSqlMap(new TestTablesServiceSqlMap(platform, createSqlReplacementTokens()));
    }

    // TODO support insert of blob test for postgres and informix
    public boolean insertIntoTestUseStreamLob(int id, String tableName, String lobValue) {
        if (symmetricDialect.isBlobSyncSupported()) {
            ISqlTransaction transaction = null;
            try {
                transaction = sqlTemplate.startSqlTransaction();
                boolean updated = transaction.prepareAndExecute(String.format(
                        "insert into %s (test_id, test_blob) values(?, ?)", tableName),
                        new Object[] { id, lobValue.getBytes() }, new int[] { Types.INTEGER,
                                Types.BLOB }) > 0;
                transaction.commit();
                return updated;
            } finally {
                if (transaction != null) {
                    transaction.close();
                }
            }
        } else {
            return false;
        }
    }

    // TODO support insert of blob test for postgres and informix
    public boolean updateTestUseStreamLob(int id, String tableName, String lobValue) {
        if (symmetricDialect.isBlobSyncSupported()) {
            if (!DatabaseNamesConstants.INFORMIX.equals(platform.getName())) {
                ISqlTransaction transaction = null;
                try {
                    transaction = sqlTemplate.startSqlTransaction();
                    boolean updated = transaction.prepareAndExecute(
                            String.format("update %s set test_blob=? where test_id=?", tableName),
                            new Object[] { lobValue.getBytes(), id }, new int[] { Types.BLOB,
                                    Types.INTEGER }) > 0;
                    transaction.commit();
                    return updated;
                } finally {
                    if (transaction != null) {
                        transaction.close();
                    }
                }
            } else {
                return false;
            }
        } else {
            return false;
        }
    }

    public void assertTestBlobIsInDatabase(int id, String tableName, String expected) {
        if (symmetricDialect.isBlobSyncSupported()) {
            int rowCount = sqlTemplate.queryForInt("select count(*) from "
                    + tableName + " where test_id=?", id);
            Assert.assertEquals("The " + id + " row for table " + tableName + " did not exist", 1, rowCount);
            Map<String, Object> values = sqlTemplate.queryForMap("select test_blob from "
                    + tableName + " where test_id=?", id);
            Assert.assertEquals(
                    "The blob column for test_use_stream_lob was not loaded into the client database",
                    expected, values != null && values.get("TEST_BLOB") != null ? new String(
                            (byte[]) values.get("TEST_BLOB")) : null);
        }
    }

    public void insertOrder(Order order) {
        sqlTemplate.update(getSql("insertOrderSql"), order.getOrderId(), order.getCustomerId(),
                order.getStatus(), order.getDeliverDate());
        List<OrderDetail> details = order.getOrderDetails();
        for (OrderDetail orderDetail : details) {
            sqlTemplate.update(
                    getSql("insertOrderDetailSql"),
                    new Object[] { orderDetail.getOrderId(), orderDetail.getLineNumber(),
                            orderDetail.getItemType(), orderDetail.getItemId(),
                            orderDetail.getQuantity(),
                            (DatabaseNamesConstants.SQLITE.equals(platform.getName()) ? orderDetail.getPrice().doubleValue() : orderDetail.getPrice())
                    }, new int[] {
                            Types.VARCHAR, Types.NUMERIC, Types.CHAR, Types.VARCHAR, Types.NUMERIC,
                            Types.NUMERIC });
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
        int blobType = symmetricDialect.getPlatform().getTableFromCache("test_customer", false)
                .getColumn(11).getMappedTypeCode();
        sqlTemplate.update(getSql("insertCustomerSql"),
                new Object[] { customer.getCustomerId(), customer.getName(),
                        customer.isActive() ? "1" : "0", customer.getAddress(), customer.getCity(),
                        customer.getState(), customer.getZip(), customer.getEntryTimestamp(),
                        customer.getEntryTime(), customer.getNotes(), customer.getIcon() },
                new int[] { Types.INTEGER, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                        Types.VARCHAR, Types.VARCHAR, Types.INTEGER, Types.TIMESTAMP,
                        Types.TIMESTAMP, Types.CLOB, blobType });
    }

    public int updateCustomer(int id, String column, Object value, int type) {
        return sqlTemplate.update(
                String.format("update test_customer set %s=? where customer_id=?", column),
                new Object[] { value, id }, new int[] { type, Types.NUMERIC });
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
        return sqlTemplate.queryForInt(String.format("select count(*) from %s", table));
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
        DatabaseInfo dbInfo = getSymmetricDialect().getPlatform().getDatabaseInfo();
        String quote = dbInfo.getDelimiterToken();
        String catalogSeparator = dbInfo.getCatalogSeparator();
        String schemaSeparator = dbInfo.getSchemaSeparator();
        ISqlTransaction transaction = sqlTemplate.startSqlTransaction();
        try {
            transaction.allowInsertIntoAutoIncrementColumns(true, testTriggerTable, quote, catalogSeparator, schemaSeparator);
            transaction.prepareAndExecute(getSql("insertIntoTestTriggersTableSql"), values);
            transaction.commit();
        } finally {
            transaction.allowInsertIntoAutoIncrementColumns(false, testTriggerTable, quote, catalogSeparator, schemaSeparator);
            transaction.close();
        }
    }

    public int countTestTriggersTable() {
        return sqlTemplate.queryForInt("select count(*) from test_triggers_table");
    }
}
