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
package org.jumpmind.symmetric.io.data.reader;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.util.BinaryEncoding;
import org.jumpmind.exception.IoException;
import org.jumpmind.symmetric.io.data.Batch;
import org.jumpmind.symmetric.io.data.CsvData;
import org.jumpmind.symmetric.io.data.DataContext;
import org.jumpmind.symmetric.io.data.DataEventType;
import org.jumpmind.symmetric.io.data.IDataReader;
import org.jumpmind.util.Statistics;
import org.xmlpull.v1.XmlPullParser;
import org.xmlpull.v1.XmlPullParserException;
import org.xmlpull.v1.XmlPullParserFactory;

public class XmlDataReader extends AbstractDataReader implements IDataReader {

    protected Reader reader;
    protected DataContext context;
    protected Batch batch;
    protected Table lastTable;
    protected Table table;
    protected CsvData data;
    protected String sourceNodeId;
    protected int lineNumber = 0;
    protected XmlPullParser parser;
    protected Statistics statistics = new Statistics();
    protected Object next = null;

    public XmlDataReader(InputStream is) {
        this(toReader(is));
    }

    public XmlDataReader(Reader reader) {
        this.reader = reader;
    }

    public void open(DataContext context) {
        try {
            this.lineNumber = 0;
            this.context = context;
            this.parser = XmlPullParserFactory.newInstance().newPullParser();
            this.parser.setInput(reader);
            this.next = readNext();
        } catch (XmlPullParserException e) {
            throw new RuntimeException(e);
        }
    }

    protected Object readNext() {
        try {
            boolean nullValue = false;
            Map<String, String> rowData = new LinkedHashMap<String, String>();
            String columnName = null;
            int eventType = parser.next();
            while (eventType != XmlPullParser.END_DOCUMENT) {
                switch (eventType) {
                    case XmlPullParser.TEXT:
                        if (columnName != null) {
                            if (!nullValue) {
                                rowData.put(columnName, parser.getText());
                            } else {
                                rowData.put(columnName, null);
                            }
                            nullValue = false;
                            columnName = null;
                        }
                        break;

                    case XmlPullParser.START_TAG:
                        String name = parser.getName();

                        if ("row".equalsIgnoreCase(name)) {
                            data = new CsvData();         
                            data.setDataEventType(DataEventType.INSERT);
                        } else if ("field".equalsIgnoreCase(name)) {
                            for (int i = 0; i < parser.getAttributeCount(); i++) {
                                String attributeName = parser.getAttributeName(i);
                                String attributeValue = parser.getAttributeValue(i);
                                if ("name".equalsIgnoreCase(attributeName)) {
                                    columnName = attributeValue;
                                } else if ("xsi:nil".equalsIgnoreCase(attributeName)) {
                                    nullValue = true;
                                }
                            }
                        } else if ("table_data".equalsIgnoreCase(name)) {
                            batch = new Batch();
                            batch.setBinaryEncoding(BinaryEncoding.BASE64);
                            table = new Table();
                            for (int i = 0; i < parser.getAttributeCount(); i++) {
                                String attributeName = parser.getAttributeName(i);
                                String attributeValue = parser.getAttributeValue(i);
                                if ("name".equalsIgnoreCase(attributeName)) {
                                    table.setName(attributeValue);
                                } 
                            }
                            return batch;
                        }
                        break;

                    case XmlPullParser.END_TAG:
                        name = parser.getName();
                        if ("row".equalsIgnoreCase(name)) {
                            String[] columnNames = rowData.keySet().toArray(
                                    new String[rowData.keySet().size()]);
                            for (String colName : columnNames) {
                                table.addColumn(new Column(colName));
                            }
                            String[] columnValues = rowData.values().toArray(
                                    new String[rowData.values().size()]);
                            data.putParsedData(CsvData.ROW_DATA, columnValues);
                            if (lastTable == null || !lastTable.equals(table)) {
                                lastTable = table;
                                return table;
                            } else {
                                return data;
                            }
                        } else if ("table_data".equalsIgnoreCase(name)) {
                            batch.setComplete(true);
                        } else if ("field".equalsIgnoreCase(name)) {
                            columnName = null;
                            nullValue = false;
                        }

                        break;
                }
                eventType = parser.next();
            }

            return null;
        } catch (IOException ex) {
            throw new IoException(ex);
        } catch (XmlPullParserException ex) {
            throw new RuntimeException(ex);
        }
    }

    public Batch nextBatch() {
        if (next instanceof Batch) {
            this.batch = (Batch) next;
            next = null;
            return batch;
        } else {
            next = readNext();
            if (next instanceof Batch) {
                this.batch = (Batch) next;
                next = null;
                return batch;
            }
        }
        return null;
    }

    public Table nextTable() {
        if (next instanceof Table) {
            this.table = (Table) next;
            next = data;
        } else if (next instanceof Batch) {
            return null;
        } else {
            next = readNext();
            if (next instanceof Table) {
                this.table = (Table) next;
                next = data;
            } else {
                this.table = null;
            }
        }

        if (this.table == null) {
            batch.setComplete(true);
        }
        return this.table;
    }

    public CsvData nextData() {
        if (next instanceof CsvData) {
            CsvData data = (CsvData) next;
            next = null;
            return data;
        } else {
            next = readNext();
            if (next instanceof CsvData) {
                CsvData data = (CsvData) next;
                next = null;
                return data;
            }
        }
        return null;
    }

    public void close() {
        IOUtils.closeQuietly(reader);
    }

    public Map<Batch, Statistics> getStatistics() {
        Map<Batch, Statistics> map = new HashMap<Batch, Statistics>(1);
        map.put(batch, statistics);
        return map;
    }
}
