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
