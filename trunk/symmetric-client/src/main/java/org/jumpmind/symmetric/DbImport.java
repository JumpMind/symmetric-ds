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
import java.io.InputStreamReader;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.commons.io.IOUtils;
import org.jumpmind.db.model.Database;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.DdlException;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.platform.JdbcDatabasePlatformFactory;
import org.jumpmind.db.sql.DmlStatement;
import org.jumpmind.db.sql.DmlStatement.DmlType;
import org.jumpmind.db.sql.ISqlTemplate;
import org.jumpmind.db.sql.SqlScript;
import org.jumpmind.db.util.BinaryEncoding;
import org.jumpmind.symmetric.io.data.DataProcessor;
import org.jumpmind.symmetric.io.data.reader.CsvTableDataReader;
import org.jumpmind.symmetric.io.data.writer.Conflict;
import org.jumpmind.symmetric.io.data.writer.Conflict.DetectConflict;
import org.jumpmind.symmetric.io.data.writer.Conflict.ResolveConflict;
import org.jumpmind.symmetric.io.data.writer.DatabaseWriter;
import org.jumpmind.symmetric.io.data.writer.DatabaseWriterErrorIgnorer;
import org.jumpmind.symmetric.io.data.writer.DatabaseWriterSettings;
import org.xmlpull.v1.XmlPullParser;
import org.xmlpull.v1.XmlPullParserFactory;

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

    protected IDatabasePlatform platform;

    public DbImport() {
    }

    public DbImport(IDatabasePlatform platform) {
        this.platform = platform;
    }

    public DbImport(DataSource dataSource) {
        platform = JdbcDatabasePlatformFactory.createNewPlatformInstance(dataSource, null, true);
    }
    
    public void importTables(String importData, String tableName) throws IOException {
        ByteArrayInputStream in = new ByteArrayInputStream(importData.getBytes());
        importTables(in, tableName);
        in.close();
    }    

    public void importTables(String importData) throws IOException {
        ByteArrayInputStream in = new ByteArrayInputStream(importData.getBytes());
        importTables(in);
        in.close();
    }

    public void importTables(InputStream in) throws IOException {
        importTables(in, null);
    }

    public void importTables(InputStream in, String tableName) throws IOException {
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
        if (forceImport) {
            settings.addErrorHandler(new DatabaseWriterErrorIgnorer());
        }
        return settings;
    }

    protected void importTablesFromCsv(InputStream in, String tableName) throws IOException {
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
        Database database = platform.readDatabaseFromXml(in, alterCaseToMatchDatabaseDefaultCase);        
        if (alterTables) {
            platform.alterDatabase(database, forceImport);                
        } else {
            platform.createDatabase(database, dropIfExists, forceImport);
        }
        
        // TODO: read in data from XML also
    }

    protected void importTablesFromSymXml(InputStream in) {
        InputStreamReader reader = new InputStreamReader(in);

        try {
            String tableName = null;
            String dataValue = null;
            String dmlType = null;
            String columnName = null;
            boolean nullValue = false;
            Map<String, String> rowData = new LinkedHashMap<String, String>();

            ISqlTemplate sqlTemplate = platform.getSqlTemplate();

            XmlPullParser parser = XmlPullParserFactory.newInstance().newPullParser();
            parser.setInput(reader);

            int eventType = parser.getEventType();
            while (eventType != XmlPullParser.END_DOCUMENT) {
                switch (eventType) {

                    case XmlPullParser.TEXT:
                        if (!nullValue)
                            dataValue = parser.getText();

                    case XmlPullParser.START_TAG:
                        String name = parser.getName();

                        if ("row".equalsIgnoreCase(name)) {
                            for (int i = 0; i < parser.getAttributeCount(); i++) {
                                String attributeName = parser.getAttributeName(i);
                                String attributeValue = parser.getAttributeValue(i);
                                if (attributeName.equalsIgnoreCase("entity")) {
                                    tableName = attributeValue;
                                } else if (attributeName.equalsIgnoreCase("dml")) {
                                    dmlType = attributeValue;
                                }
                            }
                        } else if ("data".equalsIgnoreCase(name)) {
                            for (int i = 0; i < parser.getAttributeCount(); i++) {
                                String attributeName = parser.getAttributeName(i);
                                String attributeValue = parser.getAttributeValue(i);
                                if ("key".equalsIgnoreCase(attributeName)) {
                                    columnName = attributeValue;
                                } else if ("dml".equalsIgnoreCase(attributeName)) {
                                    dmlType = attributeValue;
                                } else if ("xsi:nil".equalsIgnoreCase(attributeName)) {
                                    nullValue = true;
                                }
                            }
                        }
                        break;

                    case XmlPullParser.END_TAG:
                        name = parser.getName();
                        if ("row".equalsIgnoreCase(name)) {
                            Table table = platform
                                    .readTableFromDatabase(catalog, schema, tableName);
                            if (table == null) {
                                throw new RuntimeException("Unable to find table");
                            }
                            DmlStatement statement = platform.createDmlStatement(DmlType.INSERT,
                                    table);
                            Object[] sqlData = platform.getObjectValues(BinaryEncoding.HEX, table,
                                    rowData.keySet().toArray(new String[0]), rowData.values()
                                            .toArray(new String[0]), useVariableDates);

                            rowData.clear();
                            int rows = sqlTemplate.update(statement.getSql(), sqlData);
                            System.out.println(rows + " rows updated.");

                        } else if ("data".equalsIgnoreCase(name)) {
                            rowData.put(columnName, nullValue ? null : dataValue);
                            columnName = null;
                            dataValue = null;
                            nullValue = false;
                        }

                        break;
                }
                eventType = parser.next();
            }

        } catch (Exception e) {
            throw new DdlException(e);
        }

    }

    protected void importTablesFromSql(InputStream in) throws IOException {
        // TODO: SqlScript should be able to stream from standard input to run
        // large SQL script
        List<String> lines = IOUtils.readLines(in);

        SqlScript script = new SqlScript(lines, platform.getSqlTemplate(), true,
                SqlScript.QUERY_ENDS, platform.getSqlScriptReplacementTokens());
        script.execute();
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

}