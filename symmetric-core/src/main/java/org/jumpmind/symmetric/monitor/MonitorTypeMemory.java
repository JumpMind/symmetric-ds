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

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.Method;

import org.jumpmind.extension.IBuiltInExtensionPoint;
import org.jumpmind.symmetric.model.Monitor;
import org.jumpmind.symmetric.model.MonitorEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MonitorTypeMemory extends AbstractMonitorType implements IBuiltInExtensionPoint {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    protected MemoryPoolMXBean tenuredPool;

    @Override
    public String getName() {
        return "memory";
    }

    public MonitorTypeMemory() {
        for (MemoryPoolMXBean pool : ManagementFactory.getMemoryPoolMXBeans()) {
            if (pool.getType() == MemoryType.HEAP && pool.isUsageThresholdSupported()) {
                tenuredPool = pool;
                break;
            }
        }
        if (tenuredPool == null) {
            log.warn("Unable to find tenured memory pool");
        }
    }

    @Override
    public MonitorEvent check(Monitor monitor) {
        MonitorEvent event = new MonitorEvent();
        long usage = 0;
        if (tenuredPool != null) {
            usage = (long) ((double)tenuredPool.getUsage().getUsed() / (double)tenuredPool.getUsage().getMax() * 100);
        }
        event.setValue(usage);
        return event;
    }

    public String getMessage(long value, long threshold, long period) {
        long maxMemory = tenuredPool.getUsage().getMax();
        long usedMemory = tenuredPool.getUsage().getUsed();
        String text = "Memory threshold exceeded, " + usedMemory + " of " + maxMemory;

        ThreadInfo infos[] = new ThreadInfo[TOP_THREADS];
        long byteUsages[] = new long[TOP_THREADS];
        ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();
        for (long threadId : threadBean.getAllThreadIds()) {
            ThreadInfo info = threadBean.getThreadInfo(threadId);
            if (info.getThreadState() != Thread.State.TERMINATED) {
                rankTopUsage(infos, byteUsages, info, getThreadAllocatedBytes(threadBean, threadId));
            }
        }

        for (int i = 0; i < infos.length; i++) {
            text += "Top #" + (i + 1) + " memory thread " + infos[i].getThreadId() + " is using "
                    + String.format("%.1f", ((double) byteUsages[i] / 1048576f)) + "MB";
            text += logStackTrace(threadBean.getThreadInfo(infos[i].getThreadId(), MAX_STACK_DEPTH));
        }
        return text;
    }

    protected long getThreadAllocatedBytes(ThreadMXBean threadBean, long threadId) {
        long size = 0;
        try {
            Method method = threadBean.getClass().getMethod("getThreadAllocatedBytes");
            method.setAccessible(true);
            size = (Long) method.invoke(threadBean, threadId);
        } catch (Exception ignore) {
        }
        return size;
    }

    @Override
    public boolean requiresClusterLock() {
        return false;
    }

}
