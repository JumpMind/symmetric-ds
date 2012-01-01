/*
 * Licensed to JumpMind Inc under one or more contributor 
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding 
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU Lesser General Public License (the
 * "License"); you may not use this file except in compliance
 * with the License. 
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see           
 * <http://www.gnu.org/licenses/>.
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License. 
 */

package org.jumpmind.symmetric.db.derby;

import org.jumpmind.db.sql.ISqlTransaction;
import org.jumpmind.db.util.BinaryEncoding;
import org.jumpmind.symmetric.db.AbstractSymmetricDialect;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.model.Trigger;

public class DerbySymmetricDialect extends AbstractSymmetricDialect implements ISymmetricDialect {

    public DerbySymmetricDialect() {
        this.triggerText = new DerbyTriggerText();
    }

    @Override
    protected boolean doesTriggerExistOnPlatform(String catalog, String schema, String tableName,
            String triggerName) {
        schema = schema == null ? (platform.getDefaultSchema() == null ? null : platform
                .getDefaultSchema()) : schema;
        return jdbcTemplate.queryForInt(
                "select count(*) from sys.systriggers where triggername = ?",
                new Object[] { triggerName.toUpperCase() }) > 0;
    }

    @Override
    public boolean supportsTransactionId() {
        return true;
    }

    @Override
    public boolean isBlobSyncSupported() {
        return true;
    }

    @Override
    public boolean isClobSyncSupported() {
        return true;
    }

    @Override
    public BinaryEncoding getBinaryEncoding() {
        return BinaryEncoding.BASE64;
    }

    public void disableSyncTriggers(ISqlTransaction transaction, String nodeId) {
        transaction.queryForObject(
                String.format("values %s_sync_triggers_set_disabled(1)", tablePrefix),
                Integer.class);
        if (nodeId != null) {
            transaction.queryForObject(
                    String.format("values %s_sync_node_set_disabled('%s')", tablePrefix, nodeId),
                    String.class);
        }
    }

    public void enableSyncTriggers(ISqlTransaction transaction) {
        transaction.queryForObject(
                String.format("values %s_sync_triggers_set_disabled(0)", tablePrefix),
                Integer.class);
        jdbcTemplate.queryForObject(
                String.format("values %s_sync_node_set_disabled(null)", tablePrefix), String.class);
    }

    public String getSyncTriggersExpression() {
        return String.format("%s_sync_triggers_disabled() = 0", tablePrefix);
    }

    @Override
    public String getTransactionTriggerExpression(String defaultCatalog, String defaultSchema,
            Trigger trigger) {
        return String.format("%s_transaction_id()", tablePrefix);
    }

    @Override
    public String getSelectLastInsertIdSql(String sequenceName) {
        return "values IDENTITY_VAL_LOCAL()";
    }

    @Override
    public boolean supportsGetGeneratedKeys() {
        return false;
    }

    @Override
    protected boolean allowsNullForIdentityColumn() {
        return false;
    }

    public void purge() {
    }

    @Override
    public void truncateTable(String tableName) {
        jdbcTemplate.update("delete from " + tableName);
    }

    public boolean needsToSelectLobData() {
        return true;
    }

}