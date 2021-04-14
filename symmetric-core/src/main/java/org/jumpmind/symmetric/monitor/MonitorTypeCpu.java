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
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.RuntimeMXBean;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.Method;

import org.jumpmind.extension.IBuiltInExtensionPoint;
import org.jumpmind.symmetric.model.Monitor;
import org.jumpmind.symmetric.model.MonitorEvent;

public class MonitorTypeCpu extends AbstractMonitorType implements IBuiltInExtensionPoint {

    protected OperatingSystemMXBean osBean;

    protected RuntimeMXBean runtimeBean;

    public MonitorTypeCpu() {
        osBean = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
        runtimeBean = ManagementFactory.getRuntimeMXBean();
    }
    
    @Override
    public String getName() {
        return "cpu";
    }

    @Override
    public MonitorEvent check(Monitor monitor) {
        MonitorEvent event = new MonitorEvent();
        int availableProcessors = osBean.getAvailableProcessors();
        long prevUpTime = runtimeBean.getUptime();
        long prevProcessCpuTime = getProcessCpuTime();

        try {
            Thread.sleep(500);
        } catch (Exception ignore) {
        }

        long upTime = runtimeBean.getUptime();
        long processCpuTime = getProcessCpuTime();
        long elapsedCpu = processCpuTime - prevProcessCpuTime;
        long elapsedTime = upTime - prevUpTime;
        
        long value = (long) (elapsedCpu / (elapsedTime * 1000f * availableProcessors));
        if (value >= 100) {
            event.setValue(100);
        } else {
            event.setValue(value);
        }
        
        event.setDetails(getNotificationMessage(value, 0l, 0l));
        
        return event;
    }

    protected long getProcessCpuTime() {
        long cpuTime = 0;
        try {
            Method method = osBean.getClass().getMethod("getProcessCpuTime");
            method.setAccessible(true);
            cpuTime = (Long) method.invoke(osBean);
        } catch (Exception ignore) {
        }
        return cpuTime;
    }

    protected String getNotificationMessage(long value, long threshold, long period) {
        ThreadInfo infos[] = new ThreadInfo[TOP_THREADS];
        long cpuUsages[] = new long[TOP_THREADS];
        ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();
        for (long threadId : threadBean.getAllThreadIds()) {
            ThreadInfo info = threadBean.getThreadInfo(threadId);
            if (info != null) {
                if (info.getThreadState() != Thread.State.TERMINATED) {
                    rankTopUsage(infos, cpuUsages, info, threadBean.getThreadCpuTime(threadId));
                }
            }
        }

        StringBuilder text = new StringBuilder("CPU usage is at ");
        text.append(value).append(System.lineSeparator()).append(System.lineSeparator());
        for (int i = 0; i < infos.length; i++) {
            if (infos[i] != null) {
                text.append("Top #").append((i + 1)).append(" CPU thread ").append(infos[i].getThreadName())
                    .append(" (ID ").append(infos[i].getThreadId()).append(") is using ").append((cpuUsages[i] / 1000000000f))
                    .append("s").append(System.lineSeparator());
                text.append(logStackTrace(threadBean.getThreadInfo(infos[i].getThreadId(), MAX_STACK_DEPTH)))
                    .append(System.lineSeparator()).append(System.lineSeparator());
            }
        }
        return text.toString();
    }

    @Override
    public boolean requiresClusterLock() {
        return false;
    }

}
