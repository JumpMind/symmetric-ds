/*
 * Licensed to JumpMind Inc under one or more contributor 
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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;

import javax.sql.DataSource;

import org.jumpmind.db.io.DatabaseIO;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Database;
import org.jumpmind.db.model.SortByForeignKeyComparator;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.DatabasePlatformSettings;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.platform.JdbcDatabasePlatformFactory;
import org.jumpmind.db.platform.oracle.OracleDatabasePlatform;
import org.jumpmind.db.sql.DmlStatement.DmlType;
import org.jumpmind.db.sql.ISqlRowMapper;
import org.jumpmind.db.sql.Row;
import org.jumpmind.db.util.BinaryEncoding;
import org.jumpmind.symmetric.csv.CsvWriter;

/**
 * Export the structure and data from database tables to file.
 */
public class DbExport {

    public enum Format { SQL, CSV, XML };

    public enum Compatible { DB2, DERBY, FIREBIRD, H2, HSQLDB, HSQLDB2, INFORMIX, INTERBASE, MSSQL, MYSQL, ORACLE, POSTGRESQL, SYBASE };
		
	private Format format = Format.SQL;
	
	private Compatible compatible = Compatible.ORACLE;
	
	private boolean addDropTable;
	
	private boolean noCreateInfo;
	
	private boolean noData;
	
	private boolean ignoreMissingTables;
	
	private boolean useVariableDates;

	private boolean comments;
	
	private String catalog;
	
	private String schema;

	private IDatabasePlatform platform;

    public DbExport(IDatabasePlatform platform) {
        this.platform = platform;
    }

    public DbExport(DataSource dataSource) {
        platform = JdbcDatabasePlatformFactory.createNewPlatformInstance(dataSource, new DatabasePlatformSettings());
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
        Database database = platform.readDatabase(catalog, schema, null);
        exportTables(output, database.getTables());
    }

    public void exportTables(OutputStream output, String[] tableNames) throws IOException {
        ArrayList<Table> tableList = new ArrayList<Table>();

        for (String tableName : tableNames) {
            Table table = platform.readTableFromDatabase(catalog, schema, tableName);
            if (table != null) {
                tableList.add(table);
            } else if (! ignoreMissingTables){
                throw new RuntimeException("Cannot find table " + tableName + " in catalog " + catalog +
                        " and schema " + schema);
            }
        }
        exportTables(output, tableList.toArray(new Table[tableList.size()]));
    }

    public void exportTables(OutputStream output, String tableName, String sql) throws IOException {
        Table table = platform.getDdlReader().readTable(catalog, schema, tableName, sql);
        exportTables(output, new Table[] { table }, sql);
    }

    public void exportTables(OutputStream output, Table[] tables) throws IOException {
        exportTables(output, tables, null);
    }
    
    public void exportTables(OutputStream output, Table[] tables, String sql) throws IOException {
        final Writer writer = new OutputStreamWriter(output);
        final CsvWriter csvWriter = new CsvWriter(writer, ',');
        
        Arrays.sort(tables, new SortByForeignKeyComparator());

        IDatabasePlatform target = new OracleDatabasePlatform(null, new DatabasePlatformSettings());
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        csvWriter.setEscapeMode(CsvWriter.ESCAPE_MODE_BACKSLASH);
        writeComment(writer, "SymmetricDS " + Version.version() + " " + DbExport.class.getSimpleName());
        writeComment(writer, "Catalog: " + (catalog != null ? catalog : ""));
        writeComment(writer, "Schema: " + (schema != null ? schema : ""));
        writeComment(writer, "Started on " + df.format(new Date()));
        
        if (format == Format.XML) {
            Database db = new Database();
            if (! noCreateInfo) {
                db.addTables(tables);
            }
            new DatabaseIO().write(db, writer, "dbexport");
        }

    	for (Table table : tables) {
            writeComment(writer, "Table: " + table.getName());
            
            if (! noCreateInfo) {
                Database db = new Database();
                db.addTable(table);
                if (format == Format.SQL) {
                    writer.write(target.getDdlBuilder().createTables(db, addDropTable));
                } else if (format == Format.CSV) {
                    csvWriter.writeRecord(table.getColumnNames());
                }
            }

    		if (! noData) {
    		    writeData(writer, csvWriter, platform, table, sql);
    		}
    	}

        if (format == Format.XML) {
            writer.write("</dbexport>\n");
        }
        writeComment(writer, "Completed on " + df.format(new Date()));
        output.flush();
    	csvWriter.flush();
    	writer.flush();
    }

    protected void writeData(final Writer writer, final CsvWriter csvWriter, final IDatabasePlatform platform, Table table, String sql) throws IOException {
        final Column[] columns = table.getColumns();
        if (sql == null) {
            sql = platform.createDmlStatement(DmlType.SELECT_ALL, table).getSql();
        }
        final String insertSql = platform.createDmlStatement(DmlType.INSERT, table).getSql();
        final String selectSql = sql;
        
        if (format == Format.XML){
            writer.write("<table_data name=\"" + table.getName() + "\">\n");
        }
        
        platform.getSqlTemplate().queryForObject(selectSql, new ISqlRowMapper<Object>() {
            public Object mapRow(Row row) {
                String[] values = platform.getStringValues(BinaryEncoding.HEX, columns, row, useVariableDates);
                try {
                    if (format == Format.CSV) {
                            csvWriter.writeRecord(values, true);
                    } else if (format == Format.SQL) {
                        writer.write(platform.replaceSql(insertSql, BinaryEncoding.HEX, columns, row, useVariableDates) + "\n");
                    } else if (format == Format.XML){
                        writer.write("\t<row>\n");
                        for (int i = 0; i < columns.length; i++) {
                            writer.write("\t\t<field name=\"" + columns[i].getName() + "\">" + values[i] + "</field>\n");
                        }
                        writer.write("\t</row>\n");
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                return null;
            }
        });

        if (format == Format.XML){
            writer.write("</table_data>\n");
        }    
    }
    
    protected void writeComment(Writer writer, String commentStr) throws IOException {
        if (comments) {
            if (format == Format.CSV) {
                writer.write("# " + commentStr + "\n");
            } else if (format == Format.XML) {
                writer.write("<!-- " + commentStr + " -->\n");
            } else if (format == Format.SQL) {
                writer.write("-- " + commentStr + "\n");
            }
            writer.flush();
        }
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
}
