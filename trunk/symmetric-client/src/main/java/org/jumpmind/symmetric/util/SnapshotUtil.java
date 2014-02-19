package org.jumpmind.symmetric.util;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeSet;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateFormatUtils;
import org.jumpmind.db.model.Table;
import org.jumpmind.exception.IoException;
import org.jumpmind.properties.DefaultParameterParser.ParameterMetaData;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.Version;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.common.TableConstants;
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
import org.jumpmind.symmetric.service.ITriggerRouterService;
import org.jumpmind.util.JarBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SnapshotUtil {

    protected static final Logger log = LoggerFactory.getLogger(SnapshotUtil.class);

    public static File getSnapshotDirectory(ISymmetricEngine engine) {
        File snapshotsDir = new File(engine.getParameterService().getTempDirectory(), "snapshots");
        snapshotsDir.mkdirs();
        return snapshotsDir;
    }

    public static File createSnapshot(ISymmetricEngine engine) {

        String dirName = engine.getEngineName().replaceAll(" ", "-") + "-" + new SimpleDateFormat("yyyyMMddhhmmss").format(new Date());

        File snapshotsDir = getSnapshotDirectory(engine);

        File logfile = new File(engine.getParameterService().getString(ParameterConstants.SERVER_LOG_FILE));

        File tmpDir = new File(engine.getParameterService().getTempDirectory(), dirName);
        tmpDir.mkdirs();

        if (logfile.exists() && logfile.isFile()) {
            try {
                FileUtils.copyFileToDirectory(logfile, tmpDir);
            } catch (IOException e) {
                log.warn("Failed to copy the log file to the snapshot directory", e);
            }
        } else {
            log.warn("Could not find {} to copy to the snapshot directory",
                    logfile.getAbsolutePath());
        }

        File serviceWrapperLogFile = new File("../logs/wrapper.log");
        if (serviceWrapperLogFile.exists() && serviceWrapperLogFile.isFile()) {
            try {
                FileUtils.copyFileToDirectory(serviceWrapperLogFile, tmpDir);
            } catch (IOException e) {
                log.warn("Failed to copy the wrapper.log file to the snapshot directory", e);
            }
        } else {
            log.debug("Could not find {} to copy to the snapshot directory",
                    serviceWrapperLogFile.getAbsolutePath());
        }

        ITriggerRouterService triggerRouterService = engine.getTriggerRouterService(); 
        List<TriggerHistory> triggerHistories = triggerRouterService.getActiveTriggerHistories();
        TreeSet<Table> tables = new TreeSet<Table>();
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

        FileWriter fwriter = null;
        try {
            fwriter = new FileWriter(new File(tmpDir, "config-export.csv"));
            engine.getDataExtractorService().extractConfigurationStandalone(engine.getNodeService().findIdentity(),
                    fwriter, TableConstants.SYM_NODE, TableConstants.SYM_NODE_SECURITY,
                    TableConstants.SYM_NODE_IDENTITY, TableConstants.SYM_NODE_HOST,
                    TableConstants.SYM_NODE_CHANNEL_CTL, TableConstants.SYM_CONSOLE_USER);
        } catch (IOException e) {
            log.warn("Failed to export symmetric configuration", e);
        } finally {
            IOUtils.closeQuietly(fwriter);
        }

        FileOutputStream fos = null;
        try {
            fos = new FileOutputStream(new File(tmpDir, "table-definitions.xml"));
            DbExport export = new DbExport(engine.getDatabasePlatform());
            export.setFormat(Format.XML);
            export.setNoData(true);
            export.exportTables(fos, tables.toArray(new Table[tables.size()]));
        } catch (IOException e) {
            log.warn("Failed to export table definitions", e);
        } finally {
            IOUtils.closeQuietly(fos);
        }

        fos = null;
        try {
            fos = new FileOutputStream(new File(tmpDir, "runtime-data.xml"));
            DbExport export = new DbExport(engine.getDatabasePlatform());
            export.setFormat(Format.XML);
            export.setNoCreateInfo(true);
            String tablePrefix = engine.getTablePrefix();
            export.exportTables(
                    fos,
                    new String[] {
                            TableConstants.getTableName(tablePrefix, TableConstants.SYM_NODE_IDENTITY),
                            TableConstants.getTableName(tablePrefix, TableConstants.SYM_NODE),
                            TableConstants.getTableName(tablePrefix, TableConstants.SYM_NODE_SECURITY),
                            TableConstants.getTableName(tablePrefix, TableConstants.SYM_NODE_HOST),
                            TableConstants.getTableName(tablePrefix, TableConstants.SYM_TRIGGER_HIST),
                            TableConstants.getTableName(tablePrefix, TableConstants.SYM_LOCK),
                            TableConstants.getTableName(tablePrefix, TableConstants.SYM_NODE_COMMUNICATION)});
        } catch (IOException e) {
            log.warn("Failed to export table definitions", e);
        } finally {
            IOUtils.closeQuietly(fos);
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
        } catch (IOException e) {
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
            defaultParameters.load(in);
            IOUtils.closeQuietly(in);
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
        } catch (IOException e) {
            log.warn("Failed to export parameters-changed information", e);
        } finally {
            IOUtils.closeQuietly(fos);
        }

        writeRuntimeStats(engine, tmpDir);
        writeJobsStats(engine, tmpDir);

        fos = null;
        try {
            fos = new FileOutputStream(new File(tmpDir, "system.properties"));
            SortedProperties props = new SortedProperties();
            props.putAll(System.getProperties());
            props.store(fos, "system.properties");
        } catch (IOException e) {
            log.warn("Failed to export thread information", e);
        } finally {
            IOUtils.closeQuietly(fos);
        }

        try {
            File jarFile = new File(snapshotsDir, tmpDir.getName()
                    + ".zip");
            JarBuilder builder = new JarBuilder(tmpDir, jarFile, new File[] { tmpDir }, Version.version());
            builder.build();
            FileUtils.deleteDirectory(tmpDir);
            return jarFile;
        } catch (IOException e) {
            throw new IoException("Failed to package snapshot files into archive", e);
        }
    }
    
    protected static void writeRuntimeStats(ISymmetricEngine engine, File tmpDir) {
        FileOutputStream fos = null;
        try {
            fos = new FileOutputStream(new File(tmpDir, "runtime-stats.properties"));
            Properties runtimeProperties = new Properties();
            runtimeProperties.setProperty("unrouted.data.count",
                    Long.toString(engine.getRouterService().getUnroutedDataCount()));
            runtimeProperties.setProperty("outgoing.errors.count",
                    Long.toString(engine.getOutgoingBatchService().countOutgoingBatchesInError()));
            runtimeProperties.setProperty("outgoing.tosend.count",
                    Long.toString(engine.getOutgoingBatchService().countOutgoingBatchesUnsent()));
            runtimeProperties.setProperty("incoming.errors.count",
                    Long.toString(engine.getIncomingBatchService().countIncomingBatchesInError()));
            
            List<DataGap> gaps = engine.getDataService().findDataGaps();
            runtimeProperties.setProperty("data.gap.count",
                    Long.toString(gaps.size()));
            if (gaps.size() > 0) {
                runtimeProperties.setProperty("data.gap.start.id",
                        Long.toString(gaps.get(0).getStartId()));
                runtimeProperties.setProperty("data.gap.end.id",
                        Long.toString(gaps.get(gaps.size()-1).getStartId()));                

            }
            
            runtimeProperties.store(fos, "runtime-stats.properties");
        } catch (IOException e) {
            log.warn("Failed to export thread information", e);
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
        } catch (IOException e) {
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

    static class SortedProperties extends Properties {
        private static final long serialVersionUID = 1L;

        @Override
        public synchronized Enumeration<Object> keys() {
            return Collections.enumeration(new TreeSet<Object>(super.keySet()));
        }
    };

}
