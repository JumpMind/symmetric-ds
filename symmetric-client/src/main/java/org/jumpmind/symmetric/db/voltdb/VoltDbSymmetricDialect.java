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
package org.jumpmind.symmetric.db.voltdb;

import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.ISqlTransaction;
import org.jumpmind.db.util.BinaryEncoding;
import org.jumpmind.symmetric.db.AbstractSymmetricDialect;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.db.JdbcSymmetricDialectFactory;
import org.jumpmind.symmetric.db.postgresql.GreenplumTriggerTemplate;
import org.jumpmind.symmetric.service.IParameterService;

public class VoltDbSymmetricDialect extends AbstractSymmetricDialect {

    public VoltDbSymmetricDialect(IParameterService parameterService, IDatabasePlatform platform) {
        super(parameterService, platform);
        this.triggerTemplate = new VoltDbTriggerTemplate(this);
        this.supportsSubselectsInDelete = false;
        this.supportsSubselectsInUpdate = false;
    }

    @Override
    public void cleanDatabase() {
    }

    @Override
    public void dropRequiredDatabaseObjects() {
    }

    @Override
    public void createRequiredDatabaseObjects() {
    }

    @Override
    public BinaryEncoding getBinaryEncoding() {
        return BinaryEncoding.HEX;
    }
    
    @Override
    public void disableSyncTriggers(ISqlTransaction transaction, String nodeId) {
        // VoltDB doesn't support triggers currently.
    }
    
    @Override
    public void enableSyncTriggers(ISqlTransaction transaction) {
        // VoltDB doesn't support triggers currently.
    }

    /* (non-Javadoc)
     * @see org.jumpmind.symmetric.db.ISymmetricDialect#getSyncTriggersExpression()
     */
    @Override
    public String getSyncTriggersExpression() {
        return null;
    }
    
    @Override
    protected boolean doesTriggerExistOnPlatform(String catalogName, String schema, String tableName, String triggerName) {
        return false;
    }

}
