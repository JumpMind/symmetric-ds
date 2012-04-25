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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;

import javax.sql.DataSource;

import org.apache.commons.io.IOUtils;
import org.jumpmind.db.io.DatabaseIO;
import org.jumpmind.db.model.Database;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.platform.JdbcDatabasePlatformFactory;
import org.jumpmind.db.sql.DmlStatement;
import org.jumpmind.db.sql.DmlStatement.DmlType;
import org.jumpmind.db.sql.ISqlTemplate;
import org.jumpmind.db.sql.SqlScript;
import org.jumpmind.db.util.BinaryEncoding;
import org.jumpmind.symmetric.csv.CsvReader;

/**
 * Import data from file to database tables.
 */
public class DbImport {

    public enum Format { SQL, CSV, XML };
        
    private Format format = Format.SQL;

    private String catalog;
    
    private String schema;
    
    private boolean useVariableDates;

    private IDatabasePlatform platform;
    
    public DbImport(IDatabasePlatform platform) {
        this.platform = platform;
    }

    public DbImport(DataSource dataSource) {
        platform = JdbcDatabasePlatformFactory.createNewPlatformInstance(dataSource, null);
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
        }
    }

    protected void importTablesFromCsv(InputStream in, String tableName) throws IOException {
        ISqlTemplate sqlTemplate = platform.getSqlTemplate();
        Table table = platform.readTableFromDatabase(catalog, schema, tableName);
        if (table == null) {
            throw new RuntimeException("Unable to find table");
        }
        DmlStatement statement = platform.createDmlStatement(DmlType.INSERT, table);

        CsvReader csvReader = new CsvReader(new InputStreamReader(in));
        csvReader.setEscapeMode(CsvReader.ESCAPE_MODE_BACKSLASH);
        csvReader.setSafetySwitch(false);
        csvReader.setUseComments(true);
        csvReader.readHeaders();

        while (csvReader.readRecord()) {
            String[] values = csvReader.getValues();
            Object[] data = platform.getObjectValues(BinaryEncoding.HEX, table, csvReader.getHeaders(), values);
            for (String value : values) {
                System.out.print("|" + value);
            }
            System.out.print("\n");            
            int rows = sqlTemplate.update(statement.getSql(), data);
            System.out.println(rows + " rows updated.");
        }
        csvReader.close();
    }

    protected void importTablesFromXml(InputStream in) {
        // TODO: read in data from XML also
        Database database = new DatabaseIO().read(in);
        platform.createDatabase(database, false, true);
    }

    protected void importTablesFromSql(InputStream in) throws IOException {
        // TODO: SqlScript should be able to stream from standard input to run large SQL script
        List<String> lines = IOUtils.readLines(in);

        SqlScript script = new SqlScript(lines, platform.getSqlTemplate(), true, SqlScript.QUERY_ENDS, 
                platform.getSqlScriptReplacementTokens());
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

}