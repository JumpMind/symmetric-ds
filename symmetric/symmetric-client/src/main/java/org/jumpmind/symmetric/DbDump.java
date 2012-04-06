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

import java.io.File;
import java.io.OutputStream;
import java.text.ParseException;
import java.util.List;

import javax.swing.text.html.Option;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.db.io.DatabaseIO;
import org.jumpmind.db.model.Database;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.symmetric.io.data.Batch;
import org.jumpmind.symmetric.io.data.DataContext;
import org.jumpmind.symmetric.io.data.DataProcessor;
import org.jumpmind.symmetric.io.data.IDataWriter;
import org.jumpmind.symmetric.model.Data;

/**
 * Dump the structure and data from database tables to file.
 */
public class DbDump extends AbstractCommandLauncher {

    @SuppressWarnings("unused")
	private static final Log log = LogFactory.getLog(DbDump.class);

	private static final String OPTION_XML = "xml";
    	
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
    }
    
	@Override
	protected boolean executeOptions(CommandLine line) throws Exception {
        if (line.hasOption(OPTION_XML)) {
        	dumpSchemaAsXml(System.out);
        	return true;
        }
		return false;
	}

    public void dumpSchemaAsXml(OutputStream output) throws Exception {
    	IDatabasePlatform platform = getDatabasePlatform();
        Database db = platform.readDatabase(platform.getDefaultCatalog(), platform.getDefaultSchema(), null);
        new DatabaseIO().write(db, output);
    }

    public void dumpSchemaAsSql(OutputStream output) throws Exception {
    	IDatabasePlatform platform = getDatabasePlatform();
        Database db = platform.readDatabase(platform.getDefaultCatalog(), platform.getDefaultSchema(), null);
        output.write(platform.getDdlBuilder().createTables(db, false).getBytes());
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
