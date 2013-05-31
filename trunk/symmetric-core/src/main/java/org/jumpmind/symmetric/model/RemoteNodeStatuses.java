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
package org.jumpmind.symmetric.model;

import java.util.ArrayList;

import org.jumpmind.exception.InterruptedException;
import org.jumpmind.util.AppUtils;

public class RemoteNodeStatuses extends ArrayList<RemoteNodeStatus> {

    private static final long serialVersionUID = 1L;

    public boolean wasDataProcessed() {
        boolean dataProcessed = false;
        for (RemoteNodeStatus status : this) {
            dataProcessed |= status.getDataProcessed() > 0;
        }
        return dataProcessed;
    }

    public boolean wasBatchProcessed() {
        boolean batchProcessed = false;
        for (RemoteNodeStatus status : this) {
            batchProcessed |= status.getBatchesProcessed() > 0;
        }
        return batchProcessed;
    }

    public long getDataProcessedCount() {
        long dataProcessed = size() > 0 ? 0 : -1l;
        for (RemoteNodeStatus status : this) {
            dataProcessed += status.getDataProcessed();
        }
        return dataProcessed;
    }

    public boolean errorOccurred() {
        boolean errorOccurred = false;
        for (RemoteNodeStatus status : this) {
            errorOccurred |= status.failed();
        }
        return errorOccurred;
    }

    public RemoteNodeStatus add(Node node) {
        RemoteNodeStatus status = null;
        if (node != null) {
            status = new RemoteNodeStatus(node.getNodeId());
            add(status);
        }
        return status;
    }

    public boolean isComplete() {
        boolean complete = false;
        for (RemoteNodeStatus status : this) {
            complete |= status.isComplete();
        }
        return complete;
    }

    public void waitForComplete(long timeout) {
        long ts = System.currentTimeMillis();
        while (!isComplete() && System.currentTimeMillis() - ts < timeout) {
            AppUtils.sleep(50);
        }

        if (!isComplete()) {
            throw new InterruptedException(String.format(
                    "Timed out after %sms", timeout));
        }
    }
}
