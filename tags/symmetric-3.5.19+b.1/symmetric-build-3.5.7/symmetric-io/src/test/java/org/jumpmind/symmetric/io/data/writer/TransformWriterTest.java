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
package org.jumpmind.symmetric.io.data.writer;

import java.util.List;

import org.jumpmind.db.DbTestUtils;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Table;
import org.jumpmind.symmetric.io.data.CsvData;
import org.jumpmind.symmetric.io.data.DataEventType;
import org.jumpmind.symmetric.io.data.transform.TransformColumn;
import org.jumpmind.symmetric.io.data.transform.TransformPoint;
import org.jumpmind.symmetric.io.data.transform.TransformTable;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TransformWriterTest extends AbstractWriterTest {

    protected MockDataWriter mockWriter = new MockDataWriter();

    @BeforeClass
    public static void setup() throws Exception {
        platform = DbTestUtils.createDatabasePlatform(DbTestUtils.ROOT);
        platform.createDatabase(platform.readDatabaseFromXml("/testDatabaseWriter.xml", true),
                true, false);
    }

    @Test
    public void testNoTransform() {
        mockWriter.reset();
        Table table = new Table("original", new Column("id"));
        writeData(getTransformWriter(), new TableCsvData(table, new CsvData(DataEventType.INSERT,
                new String[] { "1" }), new CsvData(DataEventType.INSERT, new String[] { "2" })));
        List<CsvData> datas = mockWriter.writtenDatas.get(table.getFullyQualifiedTableName());
        Assert.assertEquals(2, datas.size());
        Assert.assertEquals("1", datas.get(0).getParsedData(CsvData.ROW_DATA)[0]);
        Assert.assertEquals("2", datas.get(1).getParsedData(CsvData.ROW_DATA)[0]);
    }

    @Test
    public void testTableNameChange() {
        mockWriter.reset();
        Table table = new Table("s1", new Column("id"));
        writeData(getTransformWriter(), new TableCsvData(table, new CsvData(DataEventType.INSERT,
                new String[] { "66" }), new CsvData(DataEventType.INSERT, new String[] { "77" })));
        List<CsvData> datas = mockWriter.writtenDatas.get(table.getFullyQualifiedTableName());
        Assert.assertNull(datas);
        datas = mockWriter.writtenDatas.get("t1");
        Assert.assertEquals(2, datas.size());
        Assert.assertEquals("66", datas.get(0).getParsedData(CsvData.ROW_DATA)[0]);
        Assert.assertEquals("77", datas.get(1).getParsedData(CsvData.ROW_DATA)[0]);
    }

    @Test
    public void testAddColumn() {
        mockWriter.reset();
        Table table = new Table("s2", new Column("id"));
        writeData(getTransformWriter(), new TableCsvData(table, new CsvData(DataEventType.INSERT,
                new String[] { "2" }), new CsvData(DataEventType.INSERT, new String[] { "1" })));
        List<CsvData> datas = mockWriter.writtenDatas.get(table.getFullyQualifiedTableName());
        Assert.assertNull(datas);
        datas = mockWriter.writtenDatas.get("t2");
        Assert.assertEquals(2, datas.size());
        Assert.assertEquals("2", datas.get(0).getParsedData(CsvData.ROW_DATA)[0]);
        Assert.assertEquals("1", datas.get(1).getParsedData(CsvData.ROW_DATA)[0]);
        Assert.assertEquals("added", datas.get(0).getParsedData(CsvData.ROW_DATA)[1]);
        Assert.assertEquals("added", datas.get(1).getParsedData(CsvData.ROW_DATA)[1]);

    }

    @Test
    public void testSimpleTableBeanShellMapping() throws Exception {
    }

    @Test
    public void testTwoTablesMappedToOneInsert() throws Exception {
    }

    @Test
    public void testTwoTablesMappedToOneDeleteUpdates() throws Exception {
    }

    @Test
    public void testIgnoreRowExceptionFromBshMapping() throws Exception {
    }

    protected TransformWriter getTransformWriter() {
        return new TransformWriter(platform, TransformPoint.LOAD, mockWriter, new TransformTable[] {
                new TransformTable("s1", "t1", TransformPoint.LOAD, new TransformColumn("id", "id",
                        true)),
                new TransformTable("s2", "t2", TransformPoint.LOAD, new TransformColumn("id", "id",
                        true), new TransformColumn(null, "col2", false, "const", "added")) });
    }

}
