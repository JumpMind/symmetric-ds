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
package org.jumpmind.symmetric.transform;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.jumpmind.symmetric.common.csv.CsvConstants;
import org.jumpmind.symmetric.csv.CsvWriter;
import org.jumpmind.symmetric.load.AbstractDataLoaderTest;
import org.jumpmind.symmetric.test.TestConstants;
import org.junit.Test;

public class TransformDataLoaderTest extends AbstractDataLoaderTest {

    protected final static String TEST_TABLE = "TEST_TRANSFORM_A";

    protected final static String[] TEST_KEYS = { "KEY" };

    protected final static String[] TEST_COLUMNS = { "KEY", "COLUMN_1" };

    public TransformDataLoaderTest() throws Exception {
    }

    @Test
    public void testSimpleTableMapping() throws Exception {

        cleanSlate();

        ByteArrayOutputStream out = getSimpleTransformCsv();

        TransformDataLoader transformDataLoader = new TransformDataLoader();
        transformDataLoader.setDbDialect(getDbDialect());
        transformDataLoader.setTransformService(new ITransformService() {
            public Map<String, List<TransformTable>> findTransformsFor(String nodeGroupId,
                    boolean useCache) {
                Map<String, List<TransformTable>> map = new HashMap<String, List<TransformTable>>();
                List<TransformTable> list = new ArrayList<TransformTable>();
                TransformTable transformToB = new TransformTable();
                transformToB.setSourceTableName(TEST_TABLE);
                transformToB.setTargetTableName("TEST_TRANSFORM_B");
                TransformColumn column1B = new TransformColumn();
                column1B.setSourceColumnName("KEY");
                column1B.setTargetColumnName("ID");
                column1B.setPk(true);
                transformToB.addTransformColumn(column1B);

                TransformColumn column2B = new TransformColumn();
                column2B.setSourceColumnName("COLUMN_1");
                column2B.setTargetColumnName("STRING_TWO_VALUE");
                transformToB.addTransformColumn(column2B);

                list.add(transformToB);
                
                TransformTable transformToA = new TransformTable();
                transformToA.setSourceTableName(TEST_TABLE);
                transformToA.setTargetTableName("TEST_TRANSFORM_C");
                TransformColumn column1A = new TransformColumn();
                column1A.setSourceColumnName("KEY");
                column1A.setTargetColumnName("ID");
                column1A.setPk(true);
                transformToA.addTransformColumn(column1A);

                TransformColumn column2A = new TransformColumn();
                column2A.setSourceColumnName("COLUMN_1");
                column2A.setTargetColumnName("STRING_ONE_VALUE");
                transformToA.addTransformColumn(column2A);

                list.add(transformToA);
                
                map.put(transformToA.getFullyQualifiedSourceTableName(), list);
                return map;
            }
        });

        Assert.assertEquals(0,
                getJdbcTemplate().queryForInt(String.format("select count(*) from %s", "TEST_TRANSFORM_B")));
        
        Assert.assertEquals(0,
                getJdbcTemplate().queryForInt(String.format("select count(*) from %s", "TEST_TRANSFORM_C")));

        load(out, null, transformDataLoader, transformDataLoader);

        Assert.assertEquals(1,
                getJdbcTemplate().queryForInt(String.format("select count(*) from %s where string_two_value=? and string_one_value is null", "TEST_TRANSFORM_B"), "1"));
        
        Assert.assertEquals(1,
                getJdbcTemplate().queryForInt(String.format("select count(*) from %s where  string_one_value=? and string_two_value is null", "TEST_TRANSFORM_C"), "1"));
    }

    private void cleanSlate() {
        getDbDialect().truncateTable("TEST_TRANSFORM_B");
        getDbDialect().truncateTable("TEST_TRANSFORM_C");
    }

    private ByteArrayOutputStream getSimpleTransformCsv() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CsvWriter writer = getWriter(out);
        writer.writeRecord(new String[] { CsvConstants.NODEID,
                TestConstants.TEST_CLIENT_EXTERNAL_ID });
        String nextBatchId = getNextBatchId();
        writer.writeRecord(new String[] { CsvConstants.BATCH, nextBatchId });
        writeTable(writer, TEST_TABLE, TEST_KEYS, TEST_COLUMNS);
        writer.write(CsvConstants.INSERT);
        writer.writeRecord(new String[] { "1", "1" }, true);
        writer.close();
        return out;
    }

}