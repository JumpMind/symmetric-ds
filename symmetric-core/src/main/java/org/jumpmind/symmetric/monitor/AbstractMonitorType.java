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
package org.jumpmind.symmetric.monitor;

import java.lang.management.ThreadInfo;

import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.ext.ISymmetricEngineAware;
import org.jumpmind.util.AppUtils;

public abstract class AbstractMonitorType implements IMonitorType, ISymmetricEngineAware {

    protected final int TOP_THREADS = 3;

    protected final int MAX_STACK_DEPTH = 30;

    protected ISymmetricEngine engine;
    
    @Override
    public boolean requiresClusterLock() {
        return true;
    }

    @Override
    public void setSymmetricEngine(ISymmetricEngine engine) {
        this.engine = engine;
    }

    protected void rankTopUsage(ThreadInfo infos[], long usages[], ThreadInfo info, long usage) {
        for (int i = 0; i < infos.length; i++) {
            if (usage > usages[i]) {
                for (int j = i + 1; j < infos.length; j++) {
                    infos[j] = infos[j - 1];
                    usages[j] = usages[j - 1];
                }
                infos[i] = info;
                usages[i] = usage;
                break;
            }
        }
    }

    protected String logStackTrace(ThreadInfo info) {
        StringBuilder sb = new StringBuilder();
        sb.append("Stack trace for thread ").append(info.getThreadId()).append(":\n");
        sb.append(AppUtils.formatStackTrace(info.getStackTrace()));
        return sb.toString();
    }

}
