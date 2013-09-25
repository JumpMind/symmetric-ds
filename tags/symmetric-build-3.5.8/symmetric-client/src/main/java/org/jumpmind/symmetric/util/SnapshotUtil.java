package org.jumpmind.symmetric.util;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.model.Table;
import org.jumpmind.exception.IoException;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.Version;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.common.TableConstants;
import org.jumpmind.symmetric.io.data.DbExport;
import org.jumpmind.symmetric.io.data.DbExport.Format;
import org.jumpmind.symmetric.model.TriggerHistory;
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

        List<TriggerHistory> triggerHistories = engine.getTriggerRouterService().getActiveTriggerHistories();
        List<Table> tables = new ArrayList<Table>();
        for (TriggerHistory triggerHistory : triggerHistories) {
            Table table = engine.getDatabasePlatform().getTableFromCache(triggerHistory.getSourceCatalogName(),
                    triggerHistory.getSourceSchemaName(), triggerHistory.getSourceTableName(),
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
                    TableConstants.SYM_NODE_CHANNEL_CTL);
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
                            TableConstants.getTableName(tablePrefix, TableConstants.SYM_NODE),
                            TableConstants.getTableName(tablePrefix,
                                    TableConstants.SYM_NODE_SECURITY),
                            TableConstants.getTableName(tablePrefix,
                                    TableConstants.SYM_NODE_HOST),
                            TableConstants.getTableName(tablePrefix,
                                    TableConstants.SYM_TRIGGER_HIST),
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
            effectiveParameters.store(fos, "parameters.properties");
        } catch (IOException e) {
            log.warn("Failed to export thread information", e);
        } finally {
            IOUtils.closeQuietly(fos);
        }

        fos = null;
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
            runtimeProperties.store(fos, "runtime-stats.properties");
        } catch (IOException e) {
            log.warn("Failed to export thread information", e);
        } finally {
            IOUtils.closeQuietly(fos);
        }

        fos = null;
        try {
            fos = new FileOutputStream(new File(tmpDir, "system.properties"));
            System.getProperties().store(fos, "system.properties");
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

}
