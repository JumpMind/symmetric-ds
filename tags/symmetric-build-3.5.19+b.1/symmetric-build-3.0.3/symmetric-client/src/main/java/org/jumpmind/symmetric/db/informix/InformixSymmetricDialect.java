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
 * under the License.  */

package org.jumpmind.symmetric.db.informix;

import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.ISqlTransaction;
import org.jumpmind.db.util.BinaryEncoding;
import org.jumpmind.symmetric.db.AbstractSymmetricDialect;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.model.Trigger;
import org.jumpmind.symmetric.service.IParameterService;

public class InformixSymmetricDialect extends AbstractSymmetricDialect implements ISymmetricDialect {
    
    public InformixSymmetricDialect(IParameterService parameterService, IDatabasePlatform platform) {
        super(parameterService, platform);       
        this.triggerTemplate = new InformixTriggerTemplate(this);
    }    

    @Override
    protected boolean doesTriggerExistOnPlatform(String catalog, String schema, String tableName,
            String triggerName) {
        return platform.getSqlTemplate().queryForInt(
                "select count(*) from systriggers where lower(trigname) = ?",
                new Object[] { triggerName.toLowerCase() }) > 0;
    }

    public void disableSyncTriggers(ISqlTransaction transaction, String nodeId) {
        transaction.prepareAndExecute("select " + parameterService.getTablePrefix() + "_triggers_set_disabled('t'), "
                + parameterService.getTablePrefix() + "_node_set_disabled(?) from sysmaster:sysdual",
                new Object[] { nodeId });
    }

    public void enableSyncTriggers(ISqlTransaction transaction) {
        transaction.prepareAndExecute("select " + parameterService.getTablePrefix() + "_triggers_set_disabled('f'), "
                + parameterService.getTablePrefix() + "_node_set_disabled(null) from sysmaster:sysdual");
    }

    public String getSyncTriggersExpression() {
        return "not $(defaultSchema)" + parameterService.getTablePrefix() + "_triggers_disabled()";
    }

    @Override
    public boolean supportsTransactionId() {
        // TODO: write a user-defined routine in C that calls
        // mi_get_transaction_id()
        return false;
    }

    @Override
    public boolean isTransactionIdOverrideSupported() {
        return false;
    }

    @Override
    public String getTransactionTriggerExpression(String defaultCatalog, String defaultSchema,
            Trigger trigger) {
        return "null";
    }

    @Override
    public boolean isBlobSyncSupported() {
        return false;
    }

    @Override
    public boolean isClobSyncSupported() {
        return false;
    }

    public void purge() {
    }

    @Override
    public BinaryEncoding getBinaryEncoding() {
        return BinaryEncoding.BASE64;
    }
    
}