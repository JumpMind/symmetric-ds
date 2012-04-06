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

import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.db.io.DatabaseIO;
import org.jumpmind.db.model.Database;
import org.jumpmind.db.platform.DatabasePlatformSettings;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.platform.mysql.MySqlPlatform;

/**
 * Dump the structure and data from database tables to file.
 */
public class DbDump extends AbstractCommandLauncher {

    @SuppressWarnings("unused")
	private static final Log log = LogFactory.getLog(DbDump.class);

	private static final String OPTION_XML = "xml";
	
	private static final String OPTION_COMPATIBLE = "compatible";
	
	private static final String OPTION_ADD_DROP_TABLE = "add-drop-table";
	
	private static final String OPTION_NO_CREATE_INFO = "no-create-info";
	
	private static final String OPTION_NO_DATA = "no-data";

	private static final String OPTION_COMMENTS = "comments";
	
    public DbDump(String commandName, String messageKeyPrefix) {
		super(commandName, messageKeyPrefix);
	}

	public static void main(String[] args) throws Exception {
        new DbDump("dbdump", "DbDump.Option.").execute(args);
    }
	
    protected void printHelp(Options options) {
    	System.out.println(commandName + " version " + Version.version());
    	System.out.println("Dump the structure and data from database tables to file.\n");
    	super.printHelp(options);
    }

	@Override
    protected void buildOptions(Options options) {
		super.buildOptions(options);
    	addOption(options, "x", OPTION_XML, false);
    	addOption(options, null, OPTION_COMPATIBLE, true);
    	addOption(options, null, OPTION_ADD_DROP_TABLE, false);
    	addOption(options, null, OPTION_NO_CREATE_INFO, false);
    	addOption(options, null, OPTION_NO_DATA, false);
    	addOption(options, "i", OPTION_COMMENTS, false);
    }
    
	@Override
	protected boolean executeOptions(CommandLine line) throws Exception {
		// TODO: get table names list as args
		
        if (line.hasOption(OPTION_XML)) {
        	dumpSchemaAsXml(System.out);
        } else {
	    	dumpSchemaAsSql(System.out, line.hasOption(OPTION_ADD_DROP_TABLE), line.hasOption(OPTION_NO_CREATE_INFO),
	    			line.hasOption(OPTION_NO_DATA), line.hasOption(OPTION_COMMENTS));
        }
    	return true;
	}

    public void dumpSchemaAsXml(OutputStream output) throws Exception {
    	/* TODO:
    	 * <dbdump> <database></database> </dbdump>
    	 * <table_data name="mytable"><row><field name="myfield">value</field></row></table_data>
    	 */
    	IDatabasePlatform platform = getDatabasePlatform();
        Database db = platform.readDatabase(platform.getDefaultCatalog(), platform.getDefaultSchema(), null);
        new DatabaseIO().write(db, output);
        output.flush();
    }

    public void dumpSchemaAsSql(OutputStream output, boolean addDropTable, boolean noCreateInfo, boolean noData, boolean comments) throws Exception {
    	IDatabasePlatform platform = getDatabasePlatform();
    	String catalog = platform.getDefaultCatalog();
    	String schema = platform.getDefaultSchema();
    	
        Database db = platform.readDatabase(catalog, schema, null);
        IDatabasePlatform target = new MySqlPlatform(null, new DatabasePlatformSettings());
        Writer writer = new OutputStreamWriter(output);
    	SimpleDateFormat df = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss");

    	if (comments) {
        	writer.write("-- SymmetricDS " + Version.version() + " " + commandName + "\n--\n");
        	writer.write("-- Catalog: " + (catalog == null ? "" : catalog) + " Schema: " + (schema == null ? "" : schema) + "\n");
        	writer.write("-- Started on " + df.format(new Date()) + "\n");
        	// TODO: write jdbc url?  compatible?
        }

        if (!noCreateInfo) {
        	writer.write(target.getDdlBuilder().createTables(db, addDropTable));
        }
        
        if (!noData) {
        	// TODO: dump data
        }
        
        if (comments) {
        	writer.write("-- Completed on " + df.format(new Date()) + "\n");
        }
        writer.flush();
        writer.close();
    }

//    public void copyFromTables(List<TableToExtract> tables) {
//        long batchId = 1;
//        for (TableToExtract tableToRead : tables) {
//            logger.info("(%d of %d) Copying table %s ", batchId, tables.size(), tableToRead
//                    .getTable().getTableName());
//            Batch batch = new Batch(batchId++);
//            int expectedCount = this.sourceDbDialect.getSqlTemplate().queryForInt(
//                    this.sourceDbDialect.getDataCaptureBuilder().createTableExtractCountSql(
//                            tableToRead, parameters));
//            long ts = System.currentTimeMillis();
//            DataProcessor processor = new DataProcessor(new SqlTableDataReader(
//                    this.sourceDbDialect, batch, tableToRead), getDataWriter(true, expectedCount));
//            processor.process(new DataContext(parameters));
//            long totalTableCopyTime = System.currentTimeMillis() - ts;
//            logger.info(
//                    "It took %d ms to copy %d rows from table %s.  It took %d ms to read the data and %d ms to write the data.",
//                    totalTableCopyTime, batch.getLineCount(),
//                    tableToRead.getTable().getTableName(), batch.getDataReadMillis(),
//                    batch.getDataWriteMillis());
//
//        }
//    }
//
//    protected IDataWriter getDataWriter(final boolean expectedSizeIsInRows, final long expectedSize) {
//        IDataFilter progressFilter = new IDataFilter() {
//            long statementCount = 0;
//            long currentBatchSize = 0;
//            long totalBatchSize = 0;
//            int percent = 0;
//
//            public boolean filter(DataContext context, Batch batch, Table table, Data data) {
//                statementCount++;
//                if (batch.getReadByteCount() < currentBatchSize) {
//                    totalBatchSize += currentBatchSize;
//                }
//                currentBatchSize = batch.getReadByteCount();
//                long actualSize = statementCount;
//                if (!expectedSizeIsInRows) {
//                    actualSize = currentBatchSize + totalBatchSize;
//                }
//                int currentPercent = (int) (((double) actualSize / (double) expectedSize) * 100);
//                if (currentPercent != percent) {
//                    percent = currentPercent;
//                    logger.info(buildProgressBar(percent, batch.getLineCount()));
//                }
//                return true;
//            }
//        };
//
//        if (targetFileDir != null) {
//            return new FileCsvDataWriter(this.targetFileDir, progressFilter);
//        } else {
//            return new SqlDataWriter(this.targetDbDialect, parameters, progressFilter);
//        }
//    }
//
//    protected String buildProgressBar(int percent, long lineCount) {
//        StringBuilder b = new StringBuilder("|");
//        for (int i = 1; i <= 25; i++) {
//            if (percent >= i * 4) {
//                b.append("=");
//            } else {
//                b.append(" ");
//            }
//        }
//        b.append("| ");
//        b.append(percent);
//        b.append("% Processed ");
//        b.append(lineCount);
//        b.append(" rows ");
//        b.append("\r");
//        return b.toString();
//    }

}
