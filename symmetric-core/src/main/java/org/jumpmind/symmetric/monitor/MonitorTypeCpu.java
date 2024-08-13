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

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.RuntimeMXBean;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.SystemUtils;
import org.jumpmind.extension.IBuiltInExtensionPoint;
import org.jumpmind.symmetric.model.Monitor;
import org.jumpmind.symmetric.model.MonitorEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MonitorTypeCpu extends AbstractMonitorType implements IBuiltInExtensionPoint {
    protected final Logger log = LoggerFactory.getLogger(getClass());
    protected OperatingSystemMXBean osBean;
    protected RuntimeMXBean runtimeBean;
    protected List<StackTraceElement> ignoreElements;
    public static final String NAME = "cpu";
    protected boolean useNative = true;

    public MonitorTypeCpu() {
        osBean = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
        runtimeBean = ManagementFactory.getRuntimeMXBean();
        ignoreElements = new ArrayList<StackTraceElement>();
        ignoreElements.add(new StackTraceElement("java.lang.Object", "wait", null, 0));
        ignoreElements.add(new StackTraceElement("sun.misc.Unsafe", "park", null, 0));
        ignoreElements.add(new StackTraceElement("sun.nio.ch.EPollArrayWrapper", "epollWait", null, 0));
        ignoreElements.add(new StackTraceElement("java.lang.Thread", "sleep", null, 0));
        ignoreElements.add(new StackTraceElement("sun.management.ThreadImpl", "getThreadInfo1", null, 0));
        ignoreElements.add(new StackTraceElement("sun.nio.ch.ServerSocketChannelImpl", "accept", null, 0));
        ignoreElements.add(new StackTraceElement("sun.nio.ch.ServerSocketChannelImpl", "accept0", null, 0));
    }

    @Override
    public String getName() {
        return "cpu";
    }

    @Override
    public MonitorEvent check(Monitor monitor) {
        MonitorEvent event = new MonitorEvent();
        int cpuUsage = getCpuUsage();
        log.debug("CPU usage is {}", cpuUsage);
        event.setValue(cpuUsage);
        event.setDetails(getNotificationMessage(cpuUsage, 0l, 0l));
        return event;
    }

    public int getCpuUsage() {
        int availableProcessors = osBean.getAvailableProcessors();
        log.debug("Found {} available processors", availableProcessors);
        if (useNative) {
            String line = null;
            try {
                int pid = getProcessId();
                log.debug("Checking usage of PID {}", pid);
                if (pid >= 0 && (SystemUtils.IS_OS_WINDOWS || SystemUtils.IS_OS_MAC || SystemUtils.IS_OS_LINUX)) {
                    if (SystemUtils.IS_OS_WINDOWS) {
                        line = runCommand(3, "C:\\Windows\\System32\\WindowsPowerShell\\v1.0\\powershell.exe", "-Command",
                                "Get-WmiObject -Query "
                                        + "\\\"Select * from Win32_PerfFormattedData_PerfProc_Process where IDProcess = " + pid
                                        + "\\\" | Select-Object -Property PercentProcessorTime");
                        if (line != null) {
                            return Math.min(Integer.parseInt(line.replace(" ", "")), 100);
                        }
                    } else if (SystemUtils.IS_OS_MAC) {
                        line = runCommand(25, "top", "-l2", "-pid", String.valueOf(pid));
                        if (line != null) {
                            String[] fields = line.trim().split("\\s+");
                            if (fields.length > 2) {
                                return Math.min(Math.round(Float.parseFloat(fields[2]) / availableProcessors), 100);
                            }
                        }
                    } else {
                        line = runCommand(7, "top", "-bn1", "-p", String.valueOf(pid));
                        if (line != null) {
                            String[] fields = line.trim().split("\\s+");
                            if (fields.length > 9) {
                                return Math.min(Math.round(Float.parseFloat(fields[8]) / availableProcessors), 100);
                            }
                        }
                    }
                }
            } catch (RuntimeException e) {
                log.info("Cannot parse native command line output because \"{}: {}\".  Output was: \"{}\"", e.getClass().getName(), e.getMessage(), line);
                useNative = false;
            }
            if (!useNative) {
                log.info("Switching to CPU time based on JMX");
            }
        }
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
        return Math.min((int) (elapsedCpu / (elapsedTime * 1000f * availableProcessors)), 100);
    }

    protected int getProcessId() {
        try {
            RuntimeMXBean runtime = ManagementFactory.getRuntimeMXBean();
            Field jvm = runtime.getClass().getDeclaredField("jvm");
            jvm.setAccessible(true);
            Method pid_method = jvm.get(runtime).getClass().getDeclaredMethod("getProcessId");
            pid_method.setAccessible(true);
            return (Integer) pid_method.invoke(jvm.get(runtime));
        } catch (Exception e) {
            log.debug("Caught exception", e);
            return -1;
        }
    }

    protected long getProcessCpuTime() {
        long cpuTime = 0;
        try {
            Method method = osBean.getClass().getMethod("getProcessCpuTime");
            method.setAccessible(true);
            cpuTime = (Long) method.invoke(osBean);
        } catch (Exception ignore) {
            log.debug("Caught exception", ignore);
        }
        return cpuTime;
    }

    protected String runCommand(int lineNumber, String... args) {
        String ret = null;
        List<String> cmd = new ArrayList<String>();
        for (String arg : args) {
            cmd.add(arg);
        }
        log.debug("Running command: {}", cmd);
        ProcessBuilder pb = new ProcessBuilder(cmd);
        pb.redirectErrorStream(true);
        Process process = null;
        try {
            process = pb.start();
            process.waitFor();
        } catch (Exception e) {
            log.warn("Cannot run command " + cmd + " because", e);
        }
        if (process != null) {
            ArrayList<String> cmdOutput = new ArrayList<String>();
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                String line = null;
                while ((line = reader.readLine()) != null) {
                    cmdOutput.add(line);
                }
            } catch (Exception e) {
                log.warn("Cannot read command " + cmd + " because", e);
            }
            if (cmdOutput != null && cmdOutput.size() > lineNumber) {
                ret = cmdOutput.get(lineNumber);
            }
        }
        return ret;
    }

    protected String getNotificationMessage(long value, long threshold, long period) {
        ThreadInfo infos[] = new ThreadInfo[TOP_THREADS];
        long cpuUsages[] = new long[TOP_THREADS];
        ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();
        for (long threadId : threadBean.getAllThreadIds()) {
            ThreadInfo info = threadBean.getThreadInfo(threadId, 1);
            if (info != null) {
                if (info.getThreadState() != Thread.State.TERMINATED) {
                    StackTraceElement[] trace = info.getStackTrace();
                    boolean ignore = false;
                    if (trace != null && trace.length > 0) {
                        for (StackTraceElement element : ignoreElements) {
                            if (trace[0].getClassName().equals(element.getClassName()) && trace[0].getMethodName().equals(element.getMethodName())) {
                                ignore = true;
                                break;
                            }
                        }
                    }
                    if (!ignore) {
                        rankTopUsage(infos, cpuUsages, info, threadBean.getThreadCpuTime(threadId));
                    }
                }
            }
        }
        StringBuilder text = new StringBuilder("CPU usage is at ");
        text.append(value).append("%").append(System.lineSeparator()).append(System.lineSeparator());
        for (int i = 0; i < infos.length; i++) {
            if (infos[i] != null) {
                ThreadInfo info = threadBean.getThreadInfo(infos[i].getThreadId(), MAX_STACK_DEPTH);
                if (info != null) {
                    text.append("Top #").append((i + 1)).append(" CPU thread ").append(infos[i].getThreadName())
                            .append(" (ID ").append(infos[i].getThreadId()).append(") is using ").append((cpuUsages[i] / 1000000000f))
                            .append("s").append(System.lineSeparator());
                    text.append(logStackTrace(info)).append(System.lineSeparator()).append(System.lineSeparator());
                }
            }
        }
        return text.toString();
    }

    @Override
    public boolean requiresClusterLock() {
        return false;
    }
}
