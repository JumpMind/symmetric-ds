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

import junit.framework.Assert;

import org.jumpmind.symmetric.common.csv.CsvConstants;
import org.jumpmind.symmetric.csv.CsvWriter;
import org.jumpmind.symmetric.load.AbstractDataLoaderTest;
import org.jumpmind.symmetric.test.TestConstants;
import org.junit.Before;
import org.junit.Test;

public class TransformDataLoaderTest extends AbstractDataLoaderTest {

    public TransformDataLoaderTest() throws Exception {
    }

    @Test
    public void testSimpleTableMapping() throws Exception {

        TransformDataLoader dl = getTransformDataLoader();
        load(getSimpleTransformCsv(), null, dl, dl);
        expectCount(1, "TEST_TRANSFORM_A");
        Assert.assertEquals(
                1,
                getJdbcTemplate().queryForInt(
                        "select count(*) from TEST_TRANSFORM_A where id_a=? and s1_a=? and s2_a=?",
                        1, "ONE", "CONSTANT"));
    }

    protected void expectCount(int count, String table) {
        Assert.assertEquals(count,
                getJdbcTemplate().queryForInt(String.format("select count(*) from %s", table)));
    }

    @Before
    public void cleanSlate() {
        getDbDialect().truncateTable("TEST_TRANSFORM_A");
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
        writeTable(writer, "SIMPLE", new String[] { "ID" }, new String[] { "ID", "S1", "X", "Y",
                "Z" });
        writer.write(CsvConstants.INSERT);
        writer.writeRecord(new String[] { "1", "ONE", "X", "Y", "Z" }, true);
        writer.close();
        return out;
    }

}