/**
 * Licensed to JumpMind Inc under one or more contributor
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU General Public License, version 3.0 (GPLv3)
 * (the "License"); you may not use this file except in compliance
 * with the License.
 *
 * You should have received a copy of the GNU General Public License,
 * version 3.0 (GPLv3) along with this library; if not, see
 * <http://www.gnu.org/licenses/>.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.jumpmind.symmetric.route;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

import org.jumpmind.extension.IBuiltInExtensionPoint;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.route.parse.DBFReader;

public class DBFRouter extends AbstractFileParsingRouter implements IDataRouter, IBuiltInExtensionPoint {

    private ISymmetricEngine engine;
    private DBFReader dbfReader = null;
    
    public DBFRouter(ISymmetricEngine engine) {
        this.engine = engine;
    }

    public ISymmetricEngine getEngine() {
        return this.engine;
    }
    
    @Override
    public List<String> parse(File file, int lineNumber, int tableIndex) {
        List<String> rows = new ArrayList<String>();
        
        InputStream fileInputStream = null;
        int currentLine = 1;
        try {
            boolean validateHeader = engine.getParameterService()
                    .is(ParameterConstants.DBF_ROUTER_VALIDATE_HEADER, true);
            
            fileInputStream = Files.newInputStream(file.toPath(), StandardOpenOption.READ);
            dbfReader = new DBFReader(fileInputStream, validateHeader);
            
            while (dbfReader.hasNextRecord()) {
                StringBuilder row = new StringBuilder();
                Object[] record = dbfReader.nextRecord();
                if (currentLine > lineNumber) {
                    for (int i = 0; i < record.length; i++) {
                        if (i > 0) { row.append(","); }
                        row.append(record[i]);
                    }
                    
                    rows.add(row.toString());
                }
                currentLine++;
            }
        }
        catch (Exception e) {
            log.error("Unable to parse DBF file " + file.getName() + " line number " + currentLine, e);
        }
        finally {
            if (fileInputStream != null) {
                try {
                    fileInputStream.close();
                }
                catch (IOException ioe) {
                    log.error("Unable to close file " + file.getName(), ioe);
                }
            }
        }
        return rows;
    }
    
    @Override
    public String getColumnNames() {
        StringBuffer columns = new StringBuffer();
        try {
            for (int i = 0; i < dbfReader.getFieldCount(); i++) {
                if (i > 0) { columns.append(","); }
                columns.append(dbfReader.getField(i));
            }
        }
        catch (Exception e) {
            log.error("Unable to read column names for DBF file ", e);
        }
        return columns.toString();
    }
}
