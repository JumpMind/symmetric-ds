/*
O * Licensed to JumpMind Inc under one or more contributor 
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding 
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU Lesser General Public License (the
 * "License"); you may not use this file except in compliance
 * with the License. 
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see           
 * <http://www.gnu.org/licenses/>.
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License. 
 */

package org.jumpmind.symmetric;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.platform.JdbcDatabasePlatformFactory;
import org.jumpmind.db.util.BinaryEncoding;
import org.jumpmind.exception.IoException;
import org.jumpmind.symmetric.io.data.DataProcessor;
import org.jumpmind.symmetric.io.data.reader.CsvTableDataReader;
import org.jumpmind.symmetric.io.data.reader.SqlDataReader;
import org.jumpmind.symmetric.io.data.reader.SymXmlDataReader;
import org.jumpmind.symmetric.io.data.reader.XmlDataReader;
import org.jumpmind.symmetric.io.data.writer.Conflict;
import org.jumpmind.symmetric.io.data.writer.Conflict.DetectConflict;
import org.jumpmind.symmetric.io.data.writer.Conflict.ResolveConflict;
import org.jumpmind.symmetric.io.data.writer.DatabaseWriter;
import org.jumpmind.symmetric.io.data.writer.DatabaseWriterErrorIgnorer;
import org.jumpmind.symmetric.io.data.writer.DatabaseWriterSettings;
import org.jumpmind.symmetric.io.data.writer.IDatabaseWriterFilter;

/**
 * Import data from file to database tables.
 */
public class DbImport {

    public enum Format {
        SQL, CSV, XML, SYM_XML
    };

    private Format format = Format.SQL;

    private String catalog;

    private String schema;

    private long commitRate = 10000;

    private boolean useVariableDates = false;

    /**
     * Force the import to continue, regardless of errors that might occur.
     */
    private boolean forceImport = false;

    /**
     * If a row already exists, then replace it using an update statement.
     */
    private boolean replaceRows = false;

    /**
     * Ignore rows that already exist.
     */
    private boolean ignoreCollisions = false;

    private boolean alterCaseToMatchDatabaseDefaultCase = false;

    private boolean alterTables = false;

    private boolean dropIfExists = false;
    
    private boolean ignoreMissingTables = true;

    protected IDatabasePlatform platform;
    
    protected List<IDatabaseWriterFilter> databaseWriterFilters;

    public DbImport() {
        this.databaseWriterFilters = new ArrayList<IDatabaseWriterFilter>();
    }

    public DbImport(IDatabasePlatform platform) {
        this();
        this.platform = platform;
    }

    public DbImport(DataSource dataSource) {
        this();
        platform = JdbcDatabasePlatformFactory.createNewPlatformInstance(dataSource, null, true);
    }

    public void importTables(String importData, String tableName) {
        try {
            ByteArrayInputStream in = new ByteArrayInputStream(importData.getBytes());
            importTables(in, tableName);
            in.close();
        } catch (IOException e) {
            throw new IoException(e);
        }
    }

    public void importTables(String importData) {
        try {
            ByteArrayInputStream in = new ByteArrayInputStream(importData.getBytes());
            importTables(in);
            in.close();
        } catch (IOException e) {
            throw new IoException(e);
        }

    }

    public void importTables(InputStream in) {
        importTables(in, null);
    }

    public void importTables(InputStream in, String tableName) {
        if (format == Format.SQL) {
            importTablesFromSql(in);
        } else if (format == Format.CSV) {
            importTablesFromCsv(in, tableName);
        } else if (format == Format.XML) {
            importTablesFromXml(in);
        } else if (format == Format.SYM_XML) {
            importTablesFromSymXml(in);
        }
    }

    protected Conflict buildConflictSettings() {
        Conflict conflict = new Conflict();
        conflict.setDetectType(DetectConflict.USE_PK_DATA);
        if (replaceRows) {
            conflict.setResolveType(ResolveConflict.FALLBACK);
        } else if (forceImport || ignoreCollisions) {
            conflict.setResolveType(ResolveConflict.IGNORE);
        } else {
            conflict.setResolveType(ResolveConflict.MANUAL);
        }
        return conflict;
    }

    protected DatabaseWriterSettings buildDatabaseWriterSettings() {
        DatabaseWriterSettings settings = new DatabaseWriterSettings();
        settings.setMaxRowsBeforeCommit(commitRate);        
        settings.setDefaultConflictSetting(buildConflictSettings());
        settings.setUsePrimaryKeysFromSource(false);
        settings.setAlterTable(alterTables);
        settings.setCreateTableDropFirst(dropIfExists);
        settings.setCreateTableFailOnError(!forceImport);
        settings.setDatabaseWriterFilters(databaseWriterFilters);
        settings.setIgnoreMissingTables(ignoreMissingTables);
        settings.setCreateTableAlterCaseToMatchDatabaseDefault(alterCaseToMatchDatabaseDefaultCase);
        if (forceImport) {
            settings.addErrorHandler(new DatabaseWriterErrorIgnorer());
        }
        return settings;
    }

    protected void importTablesFromCsv(InputStream in, String tableName) {
        Table table = platform.readTableFromDatabase(catalog, schema, tableName);
        if (table == null) {
            throw new RuntimeException("Unable to find table");
        }

        CsvTableDataReader reader = new CsvTableDataReader(BinaryEncoding.HEX, table.getCatalog(),
                table.getSchema(), table.getName(), in);
        DatabaseWriter writer = new DatabaseWriter(platform, buildDatabaseWriterSettings());
        DataProcessor dataProcessor = new DataProcessor(reader, writer);
        dataProcessor.process();
    }

    protected void importTablesFromXml(InputStream in) {        
        XmlDataReader reader = new XmlDataReader(in);
        DatabaseWriter writer = new DatabaseWriter(platform, buildDatabaseWriterSettings());
        DataProcessor dataProcessor = new DataProcessor(reader, writer);
        dataProcessor.process();
    }
    
    protected void importTablesFromSymXml(InputStream in) {
        SymXmlDataReader reader = new SymXmlDataReader(in);
        DatabaseWriter writer = new DatabaseWriter(platform, buildDatabaseWriterSettings());
        DataProcessor dataProcessor = new DataProcessor(reader, writer);
        dataProcessor.process();
    }

    protected void importTablesFromSql(InputStream in) {
        SqlDataReader reader = new SqlDataReader(in);
        DatabaseWriter writer = new DatabaseWriter(platform, buildDatabaseWriterSettings());
        DataProcessor dataProcessor = new DataProcessor(reader, writer);
        dataProcessor.process();
    }

    public Format getFormat() {
        return format;
    }

    public void setFormat(Format format) {
        this.format = format;
    }

    public String getCatalog() {
        return catalog;
    }

    public void setCatalog(String catalog) {
        this.catalog = catalog;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }
    
    public void setIgnoreMissingTables(boolean ignoreMissingTables) {
        this.ignoreMissingTables = ignoreMissingTables;
    }
    
    public boolean isIgnoreMissingTables() {
        return ignoreMissingTables;
    }

    public IDatabasePlatform getPlatform() {
        return platform;
    }

    public void setPlatform(IDatabasePlatform platform) {
        this.platform = platform;
    }

    public boolean isUseVariableDates() {
        return useVariableDates;
    }

    public void setUseVariableForDates(boolean useVariableDates) {
        this.useVariableDates = useVariableDates;
    }

    public void setAlterCaseToMatchDatabaseDefaultCase(boolean alterCaseToMatchDatabaseDefaultCase) {
        this.alterCaseToMatchDatabaseDefaultCase = alterCaseToMatchDatabaseDefaultCase;
    }

    public boolean isAlterCaseToMatchDatabaseDefaultCase() {
        return alterCaseToMatchDatabaseDefaultCase;
    }

    public void setCommitRate(long commitRate) {
        this.commitRate = commitRate;
    }

    public long getCommitRate() {
        return commitRate;
    }

    public void setDataSource(DataSource dataSource) {
        platform = JdbcDatabasePlatformFactory.createNewPlatformInstance(dataSource, null, true);
    }

    public void setForceImport(boolean forceImport) {
        this.forceImport = forceImport;
    }

    public boolean isForceImport() {
        return forceImport;
    }

    public void setIgnoreCollisions(boolean ignoreConflicts) {
        this.ignoreCollisions = ignoreConflicts;
    }

    public boolean isIgnoreCollisions() {
        return ignoreCollisions;
    }

    public void setReplaceRows(boolean replaceRows) {
        this.replaceRows = replaceRows;
    }

    public boolean isReplaceRows() {
        return replaceRows;
    }

    public void setAlterTables(boolean alterTables) {
        this.alterTables = alterTables;
    }

    public boolean isAlterTables() {
        return alterTables;
    }

    public void setDropIfExists(boolean dropIfExists) {
        this.dropIfExists = dropIfExists;
    }

    public boolean isDropIfExists() {
        return dropIfExists;
    }
    
    public void addDatabaseWriterFilter(IDatabaseWriterFilter filter) {
        databaseWriterFilters.add(filter);
    }

    public void removeDatabaseWriterFilter(IDatabaseWriterFilter filter) {
        databaseWriterFilters.remove(filter);
    }


}