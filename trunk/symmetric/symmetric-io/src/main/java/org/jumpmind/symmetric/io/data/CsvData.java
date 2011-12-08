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
package org.jumpmind.symmetric.io.data;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

import org.jumpmind.symmetric.csv.CsvReader;
import org.jumpmind.util.IoException;

/**
 * Holder for references to both parsed and unparsed CSV data.
 */
public class CsvData {

    public static final String OLD_DATA = "oldData";
    public static final String ROW_DATA = "rowData";
    public static final String PK_DATA = "pkData";    
    
    public static final String ATTRIBUTE_CHANNEL_IDENTIFIER = "channelId";

    private Map<String, String[]> parsedCsvData = null;

    private Map<String, String> csvData = null;
    
    private Map<String, String> attributes;
    
    protected DataEventType dataEventType;
    
    public void setDataEventType(DataEventType dataEventType) {
        this.dataEventType = dataEventType;
    }
    
    public DataEventType getDataEventType() {
        return dataEventType;
    }
    
    public void putAttribute(String attributeName, String attributeValue) {
        if (attributes == null) {
            attributes = new HashMap<String, String>();
        }
        attributes.put(attributeName, attributeValue);
    }
    
    public String getAttribute(String attributeName) {
        return attributes == null ? null : attributes.get(attributeName);
    }

    public void removeData(String key) {
        if (parsedCsvData != null) {
            parsedCsvData.remove(key);
        }

        if (csvData != null) {
            csvData.remove(key);
        }
    }

    public void putCsvData(String key, String data) {
        if (csvData == null) {
            csvData = new HashMap<String, String>(2);
        }
        csvData.put(key, data);
    }

    public String getCsvData(String key) {
        String data = null;
        if (csvData != null) {
            data = csvData.get(key);
        }

        if (data == null && parsedCsvData != null) {
            String[] parsedData = parsedCsvData.get(key);
            if (parsedData != null) {
                data = CsvUtils.escapeCsvData(parsedData);
            }
        }
        return data;
    }

    public void putParsedData(String key, String[] data) {
        if (parsedCsvData == null) {
            parsedCsvData = new HashMap<String, String[]>(2);
        }
        parsedCsvData.put(key, data);
    }

    public String[] getParsedData(String key) {
        String[] values = null;
        if (parsedCsvData != null && parsedCsvData.containsKey(key)) {
            values = parsedCsvData.get(key);
        } else if (csvData != null && csvData.containsKey(key)) {
            String data = csvData.get(key);
            try {
                CsvReader csvReader = CsvUtils.getCsvReader(new StringReader(data));
                if (csvReader.readRecord()) {
                    values = csvReader.getValues();
                    putParsedData(key, values);
                } else {
                    throw new IllegalStateException(String.format(
                            "Could not parse the data passed in: %s", data));
                }
            } catch (IOException e) {
                throw new IoException(e);
            }
        }
        return values;
    }

}