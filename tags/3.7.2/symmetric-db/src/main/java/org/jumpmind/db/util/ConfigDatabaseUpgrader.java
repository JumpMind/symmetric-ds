package org.jumpmind.db.util;

import org.jumpmind.db.model.Database;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.platform.IDdlBuilder;
import org.jumpmind.db.sql.LogSqlResultsListener;
import org.jumpmind.db.sql.SqlScript;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfigDatabaseUpgrader {

    protected static final Logger log = LoggerFactory.getLogger(ConfigDatabaseUpgrader.class);

    protected String tablePrefix = "SYM";

    protected IDatabasePlatform configDatabasePlatform;

    protected boolean logOutput = true;

    protected String schemaXml;

    public ConfigDatabaseUpgrader(String schemaXml, IDatabasePlatform configDatabasePlatform,
            boolean logOutput, String tablePrefix) {
        this.configDatabasePlatform = configDatabasePlatform;
        this.logOutput = logOutput;
        this.schemaXml = schemaXml;
        this.tablePrefix = tablePrefix;
    }

    public ConfigDatabaseUpgrader() {
    }

    public boolean upgrade() {
        try {

            if (logOutput) {
                log.info("Checking if config tables need created or altered");
            }

            Database modelFromXml = configDatabasePlatform.readDatabaseFromXml(schemaXml, true);

            configDatabasePlatform.prefixDatabase(tablePrefix, modelFromXml);
            Database modelFromDatabase = configDatabasePlatform.readFromDatabase(modelFromXml
                    .getTables());

            IDdlBuilder builder = configDatabasePlatform.getDdlBuilder();
            if (builder.isAlterDatabase(modelFromDatabase, modelFromXml)) {
                if (logOutput) {
                    log.info("There are config tables that needed altered");
                }
                String delimiter = configDatabasePlatform.getDatabaseInfo()
                        .getSqlCommandDelimiter();

                String alterSql = builder.alterDatabase(modelFromDatabase, modelFromXml);

                log.debug("Alter SQL generated: {}", alterSql);

                SqlScript script = new SqlScript(alterSql, configDatabasePlatform.getSqlTemplate(),
                        true, false, false, delimiter, null);
                if (logOutput) {
                    script.setListener(new LogSqlResultsListener(log));
                }
                script.execute(configDatabasePlatform.getDatabaseInfo()
                        .isRequiresAutoCommitForDdl());

                if (logOutput) {
                    log.info("Done with auto update of config tables");
                }
                return true;
            } else {
                return false;
            }
        } catch (RuntimeException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public void setLogOutput(boolean logOutput) {
        this.logOutput = logOutput;
    }

    public boolean isLogOutput() {
        return logOutput;
    }

    public void setConfigDatabasePlatform(IDatabasePlatform configDatabasePlatform) {
        this.configDatabasePlatform = configDatabasePlatform;
    }

    public IDatabasePlatform getConfigDatabasePlatform() {
        return configDatabasePlatform;
    }

    public void setSchemaXml(String schemaXml) {
        this.schemaXml = schemaXml;
    }

    public String getSchemaXml() {
        return schemaXml;
    }

    public void setTablePrefix(String tablePrefix) {
        this.tablePrefix = tablePrefix;
    }

    public String getTablePrefix() {
        return tablePrefix;
    }

}
