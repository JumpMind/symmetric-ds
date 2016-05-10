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
package org.jumpmind.symmetric.util;

import static org.apache.commons.lang.StringUtils.isNotBlank;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.lang.management.MemoryUsage;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.RuntimeMXBean;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeSet;

import javax.sql.DataSource;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.log4j.Appender;
import org.apache.log4j.FileAppender;
import org.codehaus.mojo.animal_sniffer.IgnoreJRERequirement;
import org.jumpmind.db.model.Table;
import org.jumpmind.exception.IoException;
import org.jumpmind.properties.DefaultParameterParser.ParameterMetaData;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.Version;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.common.SystemConstants;
import org.jumpmind.symmetric.common.TableConstants;
import org.jumpmind.symmetric.db.firebird.FirebirdSymmetricDialect;
import org.jumpmind.symmetric.io.data.DbExport;
import org.jumpmind.symmetric.io.data.DbExport.Format;
import org.jumpmind.symmetric.job.IJob;
import org.jumpmind.symmetric.job.IJobManager;
import org.jumpmind.symmetric.model.DataGap;
import org.jumpmind.symmetric.model.Lock;
import org.jumpmind.symmetric.model.Trigger;
import org.jumpmind.symmetric.model.TriggerHistory;
import org.jumpmind.symmetric.service.IClusterService;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.service.ITriggerRouterService;
import org.jumpmind.util.JarBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@IgnoreJRERequirement
public class SnapshotUtil {

    protected static final Logger log = LoggerFactory.getLogger(SnapshotUtil.class);

    public static File getSnapshotDirectory(ISymmetricEngine engine) {
        File snapshotsDir = new File(engine.getParameterService().getTempDirectory(), "snapshots");
        snapshotsDir.mkdirs();
        return snapshotsDir;
    }

    public static File createSnapshot(ISymmetricEngine engine) {

        String dirName = engine.getEngineName().replaceAll(" ", "-") + "-" + new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());

        IParameterService parameterService = engine.getParameterService();
        File tmpDir = new File(parameterService.getTempDirectory(), dirName);
        tmpDir.mkdirs();
        
        File logDir = null;
        
        String parameterizedLogDir = parameterService.getString("server.log.dir");
        if (isNotBlank(parameterizedLogDir)) {
            logDir = new File(parameterizedLogDir);
        }
        
        if (logDir != null && logDir.exists()) {
            log.info("Using server.log.dir setting as the location of the log files");
        } else {
            logDir = new File("logs");

            if (!logDir.exists()) {
                File file = findSymmetricLogFile();
                if (file != null) {
                    logDir = file.getParentFile();
                }
            }

            if (!logDir.exists()) {
                logDir = new File("../logs");
            }

            if (!logDir.exists()) {
                logDir = new File("target");
            }
            
            if (logDir.exists()) {
                File[] files = logDir.listFiles();
                if (files != null) {
                    for (File file : files) {
                        if (file.getName().toLowerCase().endsWith(".log")) {
                            try {
                                FileUtils.copyFileToDirectory(file, tmpDir);
                            } catch (IOException e) {
                                log.warn("Failed to copy " + file.getName()
                                        + " to the snapshot directory", e);
                            }
                        }
                    }
                }
            }

        }

        FileWriter fwriter = null;
        try {
            fwriter = new FileWriter(new File(tmpDir, "config-export.csv"));
            engine.getDataExtractorService().extractConfigurationStandalone(engine.getNodeService().findIdentity(),
                    fwriter, TableConstants.SYM_NODE, TableConstants.SYM_NODE_SECURITY,
                    TableConstants.SYM_NODE_IDENTITY, TableConstants.SYM_NODE_HOST,
                    TableConstants.SYM_NODE_CHANNEL_CTL, TableConstants.SYM_CONSOLE_USER);
        } catch (Exception e) {
            log.warn("Failed to export symmetric configuration", e);
        } finally {
            IOUtils.closeQuietly(fwriter);
        }

        TreeSet<Table> tables = new TreeSet<Table>();
        FileOutputStream fos = null;
        try {            
            ITriggerRouterService triggerRouterService = engine.getTriggerRouterService(); 
            List<TriggerHistory> triggerHistories = triggerRouterService.getActiveTriggerHistories();
            for (TriggerHistory triggerHistory : triggerHistories) {
                Table table = engine.getDatabasePlatform().getTableFromCache(triggerHistory.getSourceCatalogName(),
                        triggerHistory.getSourceSchemaName(), triggerHistory.getSourceTableName(),
                        false);
                if (table != null && !table.getName().toUpperCase().startsWith(engine.getSymmetricDialect().getTablePrefix().toUpperCase())) {
                    tables.add(table);
                }
            }
            
            List<Trigger> triggers = triggerRouterService.getTriggers();
            for (Trigger trigger : triggers) {
                Table table = engine.getDatabasePlatform().getTableFromCache(trigger.getSourceCatalogName(),
                        trigger.getSourceSchemaName(), trigger.getSourceTableName(),
                        false);
                if (table != null) {
                    tables.add(table);
                }            
            }

            fos = new FileOutputStream(new File(tmpDir, "table-definitions.xml"));
            DbExport export = new DbExport(engine.getDatabasePlatform());
            export.setFormat(Format.XML);
            export.setNoData(true);
            export.exportTables(fos, tables.toArray(new Table[tables.size()]));
        } catch(Exception e) {
            log.warn("Failed to export table definitions", e);
        } finally {
            IOUtils.closeQuietly(fos);
        }

        String tablePrefix = engine.getTablePrefix();
        
        DbExport export = new DbExport(engine.getDatabasePlatform());
        export.setFormat(Format.CSV);
        export.setNoCreateInfo(true);
        
        extract(export, new File(tmpDir, "sym_identity.csv"), TableConstants.getTableName(tablePrefix, TableConstants.SYM_NODE_IDENTITY));
        
        extract(export, new File(tmpDir, "sym_node.csv"),  
                TableConstants.getTableName(tablePrefix, TableConstants.SYM_NODE));
        
        extract(export, new File(tmpDir, "sym_node_security.csv"), 
                TableConstants.getTableName(tablePrefix, TableConstants.SYM_NODE_SECURITY));

        extract(export, new File(tmpDir, "sym_node_host.csv"),  
                TableConstants.getTableName(tablePrefix, TableConstants.SYM_NODE_HOST));
        
        extract(export, new File(tmpDir, "sym_trigger_hist.csv"), 
                TableConstants.getTableName(tablePrefix, TableConstants.SYM_TRIGGER_HIST));
        
        extract(export, new File(tmpDir, "sym_lock.csv"),
                TableConstants.getTableName(tablePrefix, TableConstants.SYM_LOCK));
        
        extract(export, new File(tmpDir, "sym_node_communication.csv"), 
                TableConstants.getTableName(tablePrefix, TableConstants.SYM_NODE_COMMUNICATION));        
        
        extract(export, 5000, "order by create_time desc", new File(tmpDir, "sym_outgoing_batch.csv"), 
                TableConstants.getTableName(tablePrefix, TableConstants.SYM_OUTGOING_BATCH));        
        
        extract(export, 5000, "order by create_time desc", new File(tmpDir, "sym_incoming_batch.csv"), 
                TableConstants.getTableName(tablePrefix, TableConstants.SYM_INCOMING_BATCH));          

        extract(export, 5000, "order by start_id, end_id desc", new File(tmpDir, "sym_data_gap.csv"), 
                TableConstants.getTableName(tablePrefix, TableConstants.SYM_DATA_GAP));

        extract(export, 5000, "order by relative_dir, file_name", new File(tmpDir, "sym_file_snapshot.csv"), 
                TableConstants.getTableName(tablePrefix, TableConstants.SYM_FILE_SNAPSHOT));        

        if (engine.getSymmetricDialect() instanceof FirebirdSymmetricDialect) {
            final String[] monTables = { "mon$database", "mon$attachments", "mon$transactions", "mon$statements",
                    "mon$io_stats", "mon$record_stats", "mon$memory_usage", "mon$call_stack", "mon$context_variables"};
            for (String table : monTables) {
                extract(export, new File(tmpDir, "firebird-" + table + ".csv"), table);
            }
        }
        
        final int THREAD_INDENT_SPACE = 50;
        fwriter = null;
        try {
            fwriter = new FileWriter(new File(tmpDir, "threads.txt"));
            ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();
            long[] threadIds = threadBean.getAllThreadIds();
            for (long l : threadIds) {
                ThreadInfo info = threadBean.getThreadInfo(l, 100);
                if (info != null) {
                    String threadName = info.getThreadName();
                    fwriter.append(StringUtils.rightPad(threadName, THREAD_INDENT_SPACE));
                    StackTraceElement[] trace = info.getStackTrace();
                    boolean first = true;
                    for (StackTraceElement stackTraceElement : trace) {
                        if (!first) {
                            fwriter.append(StringUtils.rightPad("", THREAD_INDENT_SPACE));
                        } else {
                            first = false;
                        }
                        fwriter.append(stackTraceElement.getClassName());
                        fwriter.append(".");
                        fwriter.append(stackTraceElement.getMethodName());
                        fwriter.append("()");
                        int lineNumber = stackTraceElement.getLineNumber();
                        if (lineNumber > 0) {
                            fwriter.append(": ");
                            fwriter.append(Integer.toString(stackTraceElement.getLineNumber()));
                        }
                        fwriter.append("\n");
                    }
                    fwriter.append("\n");
                }
            }
        } catch (Exception e) {
            log.warn("Failed to export thread information", e);
        } finally {
            IOUtils.closeQuietly(fwriter);
        }

        fos = null;
        try {
            fos = new FileOutputStream(new File(tmpDir, "parameters.properties"));
            Properties effectiveParameters = engine.getParameterService().getAllParameters();
            SortedProperties parameters = new SortedProperties();
            parameters.putAll(effectiveParameters);
            parameters.remove("db.password");            
            parameters.store(fos, "parameters.properties");
        } catch (IOException e) {
            log.warn("Failed to export parameter information", e);
        } finally {
            IOUtils.closeQuietly(fos);
        }

        fos = null;
        try {
            fos = new FileOutputStream(new File(tmpDir, "parameters-changed.properties"));
            Properties defaultParameters = new Properties();
            InputStream in = SnapshotUtil.class.getResourceAsStream("/symmetric-default.properties");
            defaultParameters.load(in);
            IOUtils.closeQuietly(in);
            in = SnapshotUtil.class.getResourceAsStream("/symmetric-console-default.properties");
            if (in != null) {
                defaultParameters.load(in);
                IOUtils.closeQuietly(in);
            }
            Properties effectiveParameters = engine.getParameterService().getAllParameters();
            Properties changedParameters = new SortedProperties();
            Map<String, ParameterMetaData> parameters = ParameterConstants.getParameterMetaData();
            for (String key: parameters.keySet()) {
                String defaultValue = defaultParameters.getProperty((String) key);
                String currentValue = effectiveParameters.getProperty((String) key);
                if (defaultValue == null  && currentValue != null || (defaultValue != null && ! defaultValue.equals(currentValue))) {
                    changedParameters.put(key, currentValue == null ? "" : currentValue);
                }
            }
            changedParameters.remove("db.password");
            changedParameters.store(fos, "parameters-changed.properties");
        } catch (Exception e) {
            log.warn("Failed to export parameters-changed information", e);
        } finally {
            IOUtils.closeQuietly(fos);
        }

        writeRuntimeStats(engine, tmpDir);
        writeJobsStats(engine, tmpDir);
        
        if ("true".equals(System.getProperty(SystemConstants.SYSPROP_STANDALONE_WEB))) {
            writeDirectoryListing(engine, tmpDir);
        }

        fos = null;
        try {
            fos = new FileOutputStream(new File(tmpDir, "system.properties"));
            SortedProperties props = new SortedProperties();
            props.putAll(System.getProperties());
            props.store(fos, "system.properties");
        } catch (Exception e) {
            log.warn("Failed to export thread information", e);
        } finally {
            IOUtils.closeQuietly(fos);
        }

        try {
            File jarFile = new File(getSnapshotDirectory(engine), tmpDir.getName()
                    + ".zip");
            JarBuilder builder = new JarBuilder(tmpDir, jarFile, new File[] { tmpDir }, Version.version());
            builder.build();
            FileUtils.deleteDirectory(tmpDir);
            return jarFile;
        } catch (Exception e) {
            throw new IoException("Failed to package snapshot files into archive", e);
        }
    }
    
    protected static void extract(DbExport export, File file, String... tables) {
        extract(export, Integer.MAX_VALUE, null, file, tables);
    }
    
    protected static void extract(DbExport export, int maxRows, String whereClause, File file, String... tables) {
        FileOutputStream fos = null;
        try {
            fos = new FileOutputStream(file);
            export.setMaxRows(maxRows);
            export.setWhereClause(whereClause);
            export.exportTables(
                    fos,
                    tables);
        } catch (Exception e) {
            log.warn("Failed to export table definitions", e);
        } finally {
            IOUtils.closeQuietly(fos);
        }
    }
    
    protected static void writeDirectoryListing(ISymmetricEngine engine, File tmpDir) {
        try {
            File home = new File(System.getProperty("user.dir"));
            if (home.getName().equalsIgnoreCase("bin")) {
                home = home.getParentFile();
            }
            
            StringBuilder output = new StringBuilder();
            printDirectoryContents(home, output);
            FileUtils.write(new File(tmpDir, "directory-listing.txt"), output);
        } catch (Exception ex) {
            log.warn("Failed to output the direcetory listing" , ex);
        }
    }
    
    protected static void printDirectoryContents(File dir, StringBuilder output) throws IOException {
        output.append("\n");
        output.append(dir.getCanonicalPath());
        output.append("\n");

        File[] files = dir.listFiles();
        for (File file : files) {
            output.append("  ");
            output.append(file.canRead() ? "r" : "-");
            output.append(file.canWrite() ? "w" : "-");
            output.append(file.canExecute() ? "x" : "-");
            output.append(StringUtils.leftPad(file.length() + "", 11));
            output.append(" ");
            output.append(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date(file
                    .lastModified())));
            output.append(" ");
            output.append(file.getName());
            output.append("\n");
        }

        for (File file : files) {
            if (file.isDirectory()) {
                printDirectoryContents(file, output);
            }
        }

    }

    protected static void writeRuntimeStats(ISymmetricEngine engine, File tmpDir) {
        FileOutputStream fos = null;
        try {
            fos = new FileOutputStream(new File(tmpDir, "runtime-stats.properties"));
            Properties runtimeProperties = new Properties() {
                private static final long serialVersionUID = 1L;
                public synchronized Enumeration<Object> keys() {
                    return Collections.enumeration(new TreeSet<Object>(super.keySet()));
                }
            };

            DataSource dataSource = engine.getDatabasePlatform().getDataSource();
            if (dataSource instanceof BasicDataSource) {
                BasicDataSource dbcp = (BasicDataSource) dataSource;
                runtimeProperties.setProperty("connections.idle", String.valueOf(dbcp.getNumIdle()));
                runtimeProperties.setProperty("connections.used", String.valueOf(dbcp.getNumActive()));
                runtimeProperties.setProperty("connections.max", String.valueOf(dbcp.getMaxActive()));   
            }
            
            Runtime rt = Runtime.getRuntime();
            runtimeProperties.setProperty("memory.free", String.valueOf(rt.freeMemory()));
            runtimeProperties.setProperty("memory.used", String.valueOf(rt.totalMemory() - rt.freeMemory()));
            runtimeProperties.setProperty("memory.max", String.valueOf(rt.maxMemory()));
            
            List<MemoryPoolMXBean> memoryPools = new ArrayList<MemoryPoolMXBean>(ManagementFactory.getMemoryPoolMXBeans());
            long usedHeapMemory = 0;
            for (MemoryPoolMXBean memoryPool : memoryPools) {
                if (memoryPool.getType().equals(MemoryType.HEAP)) {
                    MemoryUsage memoryUsage = memoryPool.getCollectionUsage();
                    runtimeProperties.setProperty("memory.heap." + memoryPool.getName().toLowerCase().replaceAll(" ", "."),
                            Long.toString(memoryUsage.getUsed()));
                    usedHeapMemory += memoryUsage.getUsed();
                }
            }
            runtimeProperties.setProperty("memory.heap.total", Long.toString(usedHeapMemory));
            
            OperatingSystemMXBean osBean = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
            runtimeProperties.setProperty("os.name", System.getProperty("os.name") + " (" + System.getProperty("os.arch") + ")");
            runtimeProperties.setProperty("os.processors", String.valueOf(osBean.getAvailableProcessors()));
            runtimeProperties.setProperty("os.load.average", String.valueOf(osBean.getSystemLoadAverage()));
            
            runtimeProperties.setProperty("engine.is.started", Boolean.toString(engine.isStarted()));
            runtimeProperties.setProperty("engine.last.restart", engine.getLastRestartTime().toString());
            
            runtimeProperties.setProperty("time.server", new Date().toString());
            runtimeProperties.setProperty("time.database", new Date(engine.getSymmetricDialect().getDatabaseTime()).toString());
            runtimeProperties.setProperty("batch.unrouted.data.count",
                    Long.toString(engine.getRouterService().getUnroutedDataCount()));
            runtimeProperties.setProperty("batch.outgoing.errors.count",
                    Long.toString(engine.getOutgoingBatchService().countOutgoingBatchesInError()));
            runtimeProperties.setProperty("batch.outgoing.tosend.count",
                    Long.toString(engine.getOutgoingBatchService().countOutgoingBatchesUnsent()));
            runtimeProperties.setProperty("batch.incoming.errors.count",
                    Long.toString(engine.getIncomingBatchService().countIncomingBatchesInError()));
            
            List<DataGap> gaps = engine.getDataService().findDataGaps();
            runtimeProperties.setProperty("data.gap.count",
                    Long.toString(gaps.size()));
            if (gaps.size() > 0) {
                runtimeProperties.setProperty("data.gap.start.id",
                        Long.toString(gaps.get(0).getStartId()));
                runtimeProperties.setProperty("data.gap.end.id",
                        Long.toString(gaps.get(gaps.size()-1).getEndId()));                

            }

            runtimeProperties.put("jvm.title", Runtime.class.getPackage().getImplementationTitle());
            runtimeProperties.put("jvm.vendor", Runtime.class.getPackage().getImplementationVendor());
            runtimeProperties.put("jvm.version", Runtime.class.getPackage().getImplementationVersion());
            RuntimeMXBean runtimeMxBean = ManagementFactory.getRuntimeMXBean();
            List<String> arguments = runtimeMxBean.getInputArguments();
            runtimeProperties.setProperty("jvm.arguments", arguments.toString());
            
            runtimeProperties.store(fos, "runtime-stats.properties");
        } catch (Exception e) {
            log.warn("Failed to export runtime-stats information", e);
        } finally {
            IOUtils.closeQuietly(fos);
        }
    }
    
    protected static void writeJobsStats(ISymmetricEngine engine, File tmpDir) {
        FileWriter writer = null;
        try {
            writer = new FileWriter(new File(tmpDir, "jobs.txt"));
            IJobManager jobManager = engine.getJobManager();
            IClusterService clusterService = engine.getClusterService();
            INodeService nodeService = engine.getNodeService();
            writer.write("Clustering is " + (clusterService.isClusteringEnabled() ? "" : "not ") + 
                    "enabled and there are " + nodeService.findNodeHosts(nodeService.findIdentityNodeId()).size() +
                    " instances in the cluster\n\n");
            writer.write(StringUtils.rightPad("Job Name", 30) + StringUtils.rightPad("Schedule", 20) + 
                    StringUtils.rightPad("Status", 10) + StringUtils.rightPad("Server Id", 30) +
                    StringUtils.rightPad("Last Server Id", 30) + StringUtils.rightPad("Last Finish Time", 30) +
                    StringUtils.rightPad("Last Run Period", 20) + StringUtils.rightPad("Avg. Run Period", 20) + "\n");
            List<IJob> jobs = jobManager.getJobs();
            Map<String, Lock> locks = clusterService.findLocks();
            for (IJob job : jobs) {
                Lock lock = locks.get(job.getClusterLockName());
                String status = getJobStatus(job, lock);
                String runningServerId = lock != null ? lock.getLockingServerId() : "";
                String lastServerId = clusterService.getServerId();
                if (lock != null) {
                    lastServerId = lock.getLastLockingServerId();
                }
                String schedule = StringUtils.isBlank(job.getCronExpression()) ? Long.toString(job
                        .getTimeBetweenRunsInMs()) : job.getCronExpression();
                String lastFinishTime = getLastFinishTime(job, lock);
    
                writer.write(StringUtils.rightPad(job.getClusterLockName().replace("_", " "), 30)+ 
                        StringUtils.rightPad(schedule, 20) + StringUtils.rightPad(status, 10) + 
                        StringUtils.rightPad(runningServerId == null ? "" : runningServerId, 30) +
                        StringUtils.rightPad(lastServerId == null ? "" : lastServerId, 30) + 
                        StringUtils.rightPad(lastFinishTime == null ? "" : lastFinishTime, 30) + 
                        StringUtils.rightPad(job.getLastExecutionTimeInMs() + "", 20) + 
                        StringUtils.rightPad(job.getAverageExecutionTimeInMs() + "", 20) + "\n");
            }
        } catch (Exception e) {
            log.warn("Failed to write jobs information", e);
        } finally {
            IOUtils.closeQuietly(writer);
        }
    }

    protected static String getJobStatus(IJob job, Lock lock) {
        String status = "IDLE";
        if (lock != null) {
            if (lock.isStopped()) {
                status = "STOPPED";
            } else if (lock.getLockTime() != null) {
                status = "RUNNING";
            }
        } else {
            status = job.isRunning() ? "RUNNING" : job.isPaused() ? "PAUSED" : job
                    .isStarted() ? "IDLE" : "STOPPED";
        }
        return status;
    }

    protected static String getLastFinishTime(IJob job, Lock lock) {
        if (lock != null && lock.getLastLockTime() != null) {
            return DateFormatUtils.ISO_DATETIME_TIME_ZONE_FORMAT.format(lock.getLastLockTime());
        } else {
            return job.getLastFinishTime() == null ? null : DateFormatUtils.ISO_DATETIME_TIME_ZONE_FORMAT.format(job
                    .getLastFinishTime());
        }
    }
    
    @SuppressWarnings("unchecked")
    public static File findSymmetricLogFile() {
        Enumeration<Appender> appenders = org.apache.log4j.Logger.getRootLogger().getAllAppenders();
        while (appenders.hasMoreElements()) {
            Appender appender = appenders.nextElement();
            if (appender instanceof FileAppender) {
                FileAppender fileAppender = (FileAppender) appender;
                if (fileAppender != null) {
                    File file = new File(fileAppender.getFile());
                    if (file != null && file.exists()) {
                        return file;
                    }
                }
            }
        }
        return null;
    }


    static class SortedProperties extends Properties {
        private static final long serialVersionUID = 1L;

        @Override
        public synchronized Enumeration<Object> keys() {
            return Collections.enumeration(new TreeSet<Object>(super.keySet()));
        }
    };

}
