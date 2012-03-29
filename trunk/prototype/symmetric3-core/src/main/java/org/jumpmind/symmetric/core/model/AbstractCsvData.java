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

package org.jumpmind.symmetric.core.model;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

import org.jumpmind.symmetric.core.common.IoException;
import org.jumpmind.symmetric.core.common.StringUtils;
import org.jumpmind.symmetric.csv.CsvReader;
import org.jumpmind.symmetric.csv.CsvUtils;

/**
 * 
 */
abstract class AbstractCsvData {

    private Map<String, String[]> parsedCsvData = null;

    protected void removeData(String key) {
        parsedCsvData.remove(key);
    }

    protected void putData(String key, String[] data) {
        if (parsedCsvData == null) {
            parsedCsvData = new HashMap<String, String[]>(2);
        }
        parsedCsvData.put(key, data);
    }

    protected String[] getData(String key, String data) {
        String[] values = null;
        if (parsedCsvData != null && parsedCsvData.containsKey(key)) {
            values = parsedCsvData.get(key);
        } else if (!StringUtils.isBlank(data)) {
            try {
                CsvReader csvReader = CsvUtils.getCsvReader(new StringReader(data));
                if (csvReader.readRecord()) {
                    values = csvReader.getValues();
                    putData(key, values);
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