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
 * under the License.  */
package org.jumpmind.symmetric.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;

import org.jumpmind.symmetric.common.csv.CsvConstants;
import org.jumpmind.symmetric.common.logging.ILog;
import org.jumpmind.symmetric.common.logging.LogFactory;
import org.jumpmind.symmetric.csv.CsvReader;
import org.jumpmind.symmetric.csv.CsvWriter;


public class CsvUtils {

    static final ILog log = LogFactory.getLog(CsvUtils.class);

    public static final String DELIMITER = ", ";
    
    public static final String LINE_SEPARATOR = System.getProperty("line.separator");

    public static CsvReader getCsvReader(Reader reader) {
        CsvReader csvReader = new CsvReader(reader);
        csvReader.setEscapeMode(CsvReader.ESCAPE_MODE_BACKSLASH);
        csvReader.setSafetySwitch(false);
        return csvReader;
    }

    public static String[] tokenizeCsvData(String csvData) {
        String[] tokens = null;
        if (csvData != null) {
            InputStreamReader reader = new InputStreamReader(new ByteArrayInputStream(csvData.getBytes()));
            CsvReader csvReader = getCsvReader(reader);
            try {
                if (csvReader.readRecord()) {
                    tokens = csvReader.getValues();
                }
            } catch (IOException e) {
            }
        }
        return tokens;
    }

    public static String escapeCsvData(String data) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CsvWriter writer = new CsvWriter(new OutputStreamWriter(out), ',');
        writer.setEscapeMode(CsvWriter.ESCAPE_MODE_BACKSLASH);
        try {
            writer.write(data);
            writer.close();
            out.close();
        } catch (IOException e) {
        }
        return out.toString();
    }
    
    public static String escapeCsvData(String[] data) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CsvWriter writer = new CsvWriter(new OutputStreamWriter(out), ',');
        writer.setEscapeMode(CsvWriter.ESCAPE_MODE_BACKSLASH);        
        for (String s : data) {
            try {
                writer.write(s);
            } catch (IOException e) {
            }
        }
        writer.close();
        return out.toString();
    }

    public static int write(Writer writer, String... data) throws IOException {
        StringBuilder buffer = new StringBuilder();
        for (String string : data) {
            buffer.append(string);
        }

        writer.write(buffer.toString());
        if (log.isDebugEnabled()) {
            log.debug("BufferWriting", buffer);
        }
        return buffer.length();
    }

    public static void writeSql(String sql, Writer writer) throws IOException {
        write(writer, CsvConstants.SQL, DELIMITER, sql);
        writeLineFeed(writer);
    }
    
    public static void writeBsh(String script, Writer writer) throws IOException {
        write(writer, CsvConstants.BSH, DELIMITER, script);
        writeLineFeed(writer);    
    }
    
    public static void writeLineFeed(Writer writer) throws IOException {
        writer.write(LINE_SEPARATOR);
    }

}