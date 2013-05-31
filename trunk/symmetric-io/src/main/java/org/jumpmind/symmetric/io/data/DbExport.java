package org.jumpmind.symmetric.io.data;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.io.DatabaseXmlUtil;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Database;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.DdlBuilderFactory;
import org.jumpmind.db.platform.DmlStatementFactory;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.platform.IDdlBuilder;
import org.jumpmind.db.sql.DmlStatement.DmlType;
import org.jumpmind.db.sql.ISqlRowMapper;
import org.jumpmind.db.sql.Row;
import org.jumpmind.db.util.BinaryEncoding;
import org.jumpmind.exception.IoException;
import org.jumpmind.symmetric.csv.CsvWriter;
import org.jumpmind.symmetric.io.IoConstants;
import org.jumpmind.symmetric.io.IoVersion;

/**
 * Export the structure and data from database tables to file.
 */
public class DbExport {

    public enum Format {
        SQL, CSV, XML, SYM_XML
    };

    public enum Compatible {
        DB2, DERBY, FIREBIRD, GREENPLUM, H2, HSQLDB, HSQLDB2, INFORMIX, INTERBASE, MSSQL, MYSQL, ORACLE, POSTGRES, SYBASE, SQLITE, MARIADB
    };

    private Format format = Format.SQL;

    private Compatible compatible;

    private boolean addDropTable;

    private boolean noCreateInfo;

    private boolean noIndices;

    private boolean noForeignKeys;

    private boolean noData;

    private boolean ignoreMissingTables;

    private boolean useVariableDates;

    private boolean comments;
    
    private String whereClause;

    private String catalog;

    private String schema;

    private String dir;
    
    private boolean useQuotedIdentifiers = true;

    private IDatabasePlatform platform;

    public DbExport(IDatabasePlatform platform) {
        this.platform = platform;
        compatible = Compatible.valueOf(platform.getName().toUpperCase());
    }

    public String exportTables() throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        exportTables(output);
        output.close();
        return output.toString();
    }

    public String exportTables(String[] tableNames) throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        exportTables(output, tableNames);
        output.close();
        return output.toString();
    }

    public String exportTables(Table[] tables) throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        exportTables(output, tables);
        output.close();
        return output.toString();
    }

    public void exportTables(OutputStream output) throws IOException {
        Database database = platform.readDatabase(getCatalogToUse(), getSchemaToUse(),
                new String[] { "TABLE" });
        exportTables(output, database.getTables());
    }

    public void exportTables(OutputStream output, String[] tableNames) throws IOException {
        ArrayList<Table> tableList = new ArrayList<Table>();

        for (String tableName : tableNames) {
            Table table = platform.readTableFromDatabase(getCatalogToUse(), getSchemaToUse(),
                    tableName);
            if (table != null) {
                tableList.add(table);
            } else if (!ignoreMissingTables) {
                throw new RuntimeException("Cannot find table " + tableName + " in catalog "
                        + getCatalogToUse() + " and schema " + getSchemaToUse());
            }
        }
        exportTables(output, tableList.toArray(new Table[tableList.size()]));
    }

    public void exportTable(OutputStream output, String tableName, String sql) throws IOException {
        Table table = platform
                .readTableFromDatabase(getCatalogToUse(), getSchemaToUse(), tableName);
        exportTables(output, new Table[] { table }, sql);
    }

    public void exportTables(OutputStream output, Table[] tables) throws IOException {
        exportTables(output, tables, null);
    }

    public void exportTables(OutputStream output, Table[] tables, String sql) throws IOException {

        for (int i = 0; i < tables.length; i++) {
            // if the table definition did not come from the database, then read
            // the table from the database
            if (!tables[i].containsJdbcTypes()) {
                tables[i] = platform.readTableFromDatabase(getCatalogToUse(), getSchemaToUse(),
                        tables[i].getName());
            }
        }

        WriterWrapper writerWrapper = null;

        try {
            writerWrapper = new WriterWrapper(output);

            tables = Database.sortByForeignKeys(tables);

            for (Table table : tables) {
                writeTable(writerWrapper, table, sql);
            }
        } finally {
            if (writerWrapper != null) {
                writerWrapper.close();
            }
        }
    }

    protected String getSchemaToUse() {
        if (StringUtils.isBlank(schema)) {
            return platform.getDefaultSchema();
        } else {
            return schema;
        }
    }

    protected String getCatalogToUse() {
        if (StringUtils.isBlank(catalog)) {
            return platform.getDefaultCatalog();
        } else {
            return catalog;
        }
    }

    protected void writeTable(final WriterWrapper writerWrapper, Table table, String sql)
            throws IOException {

        writerWrapper.startTable(table);

        if (!noData) {
            if (sql == null) {
                sql = platform.createDmlStatement(DmlType.SELECT_ALL, table).getSql();
            }
            
            if (StringUtils.isNotBlank(whereClause)) {
                sql = String.format("%s %s", sql, whereClause);
            }

            platform.getSqlTemplate().query(sql, new ISqlRowMapper<Object>() {
                public Object mapRow(Row row) {
                    writerWrapper.writeRow(row);
                    return Boolean.TRUE;
                }
            });
        }

        writerWrapper.finishTable(table);

    }

    protected Database getDatabase(Table table) {
        return getDatabase(new Table[] { table });
    }

    protected Database getDatabase(Table[] tables) {
        Database db = new Database();
        try {
            if (!noCreateInfo) {
                for (Table table : tables) {
                    Table newTable = (Table) table.clone();
                    if (noIndices) {
                        newTable.removeAllIndices();
                    }
                    if (noForeignKeys) {
                        newTable.removeAllForeignKeys();
                    }
                    db.addTable(newTable);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return db;
    }

    public Format getFormat() {
        return format;
    }

    public void setFormat(Format format) {
        this.format = format;
    }

    public Compatible getCompatible() {
        return compatible;
    }

    public void setCompatible(Compatible compatible) {
        this.compatible = compatible;
    }

    public boolean isAddDropTable() {
        return addDropTable;
    }

    public void setAddDropTable(boolean addDropTable) {
        this.addDropTable = addDropTable;
    }

    public boolean isNoCreateInfo() {
        return noCreateInfo;
    }

    public void setNoCreateInfo(boolean noCreateInfo) {
        this.noCreateInfo = noCreateInfo;
    }

    public boolean isNoData() {
        return noData;
    }

    public void setNoData(boolean noData) {
        this.noData = noData;
    }
    
    public void setUseQuotedIdentifiers(boolean useQuotedIdentifiers) {
        this.useQuotedIdentifiers = useQuotedIdentifiers;
    }
    
    public boolean isUseQuotedIdentifiers() {
        return useQuotedIdentifiers;
    }
    
    public void setWhereClause(String whereClause) {
        this.whereClause = whereClause;
    }
    
    public String getWhereClause() {
        return whereClause;
    }

    public boolean isComments() {
        return comments;
    }

    public void setComments(boolean comments) {
        this.comments = comments;
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

    public boolean isIgnoreMissingTables() {
        return ignoreMissingTables;
    }

    public void setIgnoreMissingTables(boolean ignoreMissingTables) {
        this.ignoreMissingTables = ignoreMissingTables;
    }

    public boolean isUseVariableDates() {
        return useVariableDates;
    }

    public void setUseVariableForDates(boolean useVariableDates) {
        this.useVariableDates = useVariableDates;
    }

    public boolean isNoIndices() {
        return noIndices;
    }

    public void setNoIndices(boolean noIndices) {
        this.noIndices = noIndices;
    }

    public boolean isNoForeignKeys() {
        return noForeignKeys;
    }

    public void setNoForeignKeys(boolean noForeignKeys) {
        this.noForeignKeys = noForeignKeys;
    }

    public void setDir(String dir) {
        this.dir = dir;
    }

    public String getDir() {
        return dir;
    }

    class WriterWrapper {
        final private SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        private CsvWriter csvWriter;
        private Writer writer;
        private Table table;
        private String insertSql;
        private boolean startedWriting = false;

        public WriterWrapper(OutputStream os) {
            if (StringUtils.isBlank(dir) && os != null) {
                try {
                    writer = new OutputStreamWriter(os, IoConstants.ENCODING);
                } catch (UnsupportedEncodingException e) {
                    throw new IoException(e);
                }
            }
        }

        protected void startTable(Table table) {
            try {
                this.table = table;
                if (StringUtils.isNotBlank(dir)) {
                    startedWriting = false;
                    File directory = new File(dir);
                    if (!directory.exists()) {
                        directory.mkdirs();
                    }

                    File file = new File(dir, String.format("%s.%s", table.getName(), format
                            .toString().replace('_', '.').toLowerCase()));
                    FileUtils.deleteQuietly(file);
                    try {
                        writer = new FileWriter(file);
                    } catch (IOException e) {
                        throw new IoException(e);
                    }
                }

                if (!startedWriting) {
                    if (format == Format.SYM_XML) {
                        write("<batch xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">\n");
                    } else if (format == Format.XML) {
                        write("<database xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" name=\"dbexport\">\n");
                    }
                    startedWriting = true;
                }

                if (format == Format.CSV && csvWriter == null) {
                    csvWriter = new CsvWriter(writer, ',');
                    csvWriter.setEscapeMode(CsvWriter.ESCAPE_MODE_BACKSLASH);
                    csvWriter.setTextQualifier('\"');
                    csvWriter.setUseTextQualifier(true);
                    csvWriter.setForceQualifier(true);
                } else if (format == Format.SQL) {
                    Table targetTable = table.copy();
                    targetTable.setSchema(schema);
                    targetTable.setCatalog(catalog);
                    insertSql = DmlStatementFactory.createDmlStatement(
                            compatible.toString().toLowerCase(), DmlType.INSERT, targetTable, useQuotedIdentifiers)
                            .getSql();
                }

                if (!noCreateInfo) {
                    if (format == Format.SQL) {
                        IDdlBuilder target = DdlBuilderFactory.createDdlBuilder(compatible
                                .toString().toLowerCase());
                        write(target.createTables(getDatabase(table), addDropTable));
                    } else if (format == Format.XML) {
                        DatabaseXmlUtil.write(table, writer);
                    }
                }

                writeComment("DbExport: " + StringUtils.defaultString(IoVersion.getVersion().version()));
                writeComment("Catalog: " + StringUtils.defaultString(getCatalogToUse()));
                writeComment("Schema: " + StringUtils.defaultString(getSchemaToUse()));
                writeComment("Table: " + table.getName());
                writeComment("Started on " + df.format(new Date()));

                if (format == Format.CSV) {
                    csvWriter.writeRecord(table.getColumnNames());
                } else if (!noData && format == Format.XML) {
                    write("<table_data name=\"", table.getName(), "\">\n");
                }
            } catch (IOException e) {
                throw new IoException(e);
            }

        }

        protected void writeComment(String commentStr) {
            if (writer != null) {
                try {
                    if (comments) {
                        if (format == Format.CSV) {
                            write("# ", commentStr, "\n");
                        } else if (format == Format.XML) {
                            write("<!-- ", commentStr, " -->\n");
                        } else if (format == Format.SQL) {
                            write("-- ", commentStr, "\n");
                        }
                        writer.flush();
                    }
                } catch (IOException e) {
                    throw new IoException(e);
                }
            }
        }

        protected void writeRow(Row row) {
            Column[] columns = table.getColumns();
            String[] values = platform.getStringValues(BinaryEncoding.HEX, columns, row,
                    useVariableDates);
            try {
                if (format == Format.CSV) {
                    csvWriter.writeRecord(values, true);
                } else if (format == Format.SQL) {
                    write(platform.replaceSql(insertSql, BinaryEncoding.HEX, table, row,
                            useVariableDates), "\n");

                } else if (format == Format.XML) {
                    write("\t<row>\n");
                    for (int i = 0; i < columns.length; i++) {
                        if (values[i] != null) {
                            write("\t\t<field name=\"", columns[i].getName(), "\">",
                                    StringEscapeUtils.escapeXml(values[i]), "</field>\n");
                        } else {
                            write("\t\t<field name=\"", columns[i].getName(),
                                    "\" xsi:nil=\"true\" />\n");
                        }
                    }
                    write("\t</row>\n");

                } else if (format == Format.SYM_XML) {
                    write("\t<row entity=\"", table.getName(), "\" dml=\"I\">\n");
                    for (int i = 0; i < columns.length; i++) {
                        if (values[i] != null) {
                            write("\t\t<data key=\"", columns[i].getName(), "\">",
                                    StringEscapeUtils.escapeXml(values[i]), "</data>\n");
                        } else {
                            write("\t\t<data key=\"", columns[i].getName(),
                                    "\" xsi:nil=\"true\" />\n");
                        }
                    }
                    write("\t</row>\n");
                }

            } catch (IOException e) {
                throw new IoException(e);
            }

        }

        protected void write(String... data) {
            for (String string : data) {
                try {
                    writer.write(string);
                } catch (IOException e) {
                    throw new IoException(e);
                }
            }
        }

        protected void finishTable(Table table) {
            if (!noData && format == Format.XML) {
                write("</table_data>\n");
            }

            if (StringUtils.isNotBlank(dir)) {
                close();
            }
        }

        public void close() {

            writeComment("Completed on " + df.format(new Date()));

            if (format == Format.SYM_XML) {
                write("</batch>\n");
            } else if (format == Format.XML) {
                write("</database>\n");
            }

            startedWriting = false;

            if (csvWriter != null) {
                csvWriter.flush();
                csvWriter.close();
                csvWriter = null;
            }

            IOUtils.closeQuietly(writer);
            writer = null;
        }

    }

}
