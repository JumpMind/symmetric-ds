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
package org.jumpmind.symmetric.db;

import org.jumpmind.extension.IBuiltInExtensionPoint;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.common.TableConstants;
import org.jumpmind.symmetric.ext.ISymmetricEngineAware;
import org.jumpmind.symmetric.util.ModuleException;
import org.jumpmind.symmetric.util.ModuleManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SoftwareUpgradeListener implements ISoftwareUpgradeListener, ISymmetricEngineAware, IBuiltInExtensionPoint {
    private static final Logger log = LoggerFactory.getLogger(SoftwareUpgradeListener.class);
    ISymmetricEngine engine;

    @Override
    public void setSymmetricEngine(ISymmetricEngine engine) {
        this.engine = engine;
    }

    @Override
    public void upgrade(String databaseVersion, String softwareVersion) {
        if (databaseVersion.equals("3.8.0")) {
            log.info("Detected an original value of 3.8.0 performing necessary upgrades.");
            String sql = "update  " + engine.getParameterService().getTablePrefix()
                    + "_" + TableConstants.SYM_CHANNEL +
                    " set max_batch_size = 10000 where reload_flag = 1 and max_batch_size = 1";
            engine.getSqlTemplate().update(sql);
        }
        try {
            ModuleManager.getInstance().upgradeAll();
        } catch (ModuleException e) {
            throw new RuntimeException(e);
        }
    }
}
