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
package org.jumpmind.symmetric.io.data.stage;

import java.io.File;

import org.apache.commons.io.IOUtils;
import org.jumpmind.symmetric.io.stage.ThresholdFileWriter;
import org.junit.Assert;
import org.junit.Test;

public class ThresholdFileWriterTest {

    final String TEST_STR = "The quick brown fox jumped over the lazy dog";

    @Test
    public void testNoWriteToFile() throws Exception {
        File file = getTestFile();
        ThresholdFileWriter writer = new ThresholdFileWriter(TEST_STR.length() + 1, new StringBuilder(), file);
        writer.write(TEST_STR);
        Assert.assertFalse(file.exists());
        Assert.assertEquals(TEST_STR, IOUtils.toString(writer.getReader()));
        file.delete();
    }

    @Test
    public void testWriteToFile() throws Exception {
        File file = getTestFile();
        ThresholdFileWriter writer = new ThresholdFileWriter(TEST_STR.length() - 1, new StringBuilder(), file);
        writer.write(TEST_STR);
        Assert.assertTrue(file.exists());
        Assert.assertEquals(TEST_STR, IOUtils.toString(writer.getReader()));
        file.delete();
    }

    private File getTestFile() {
        File file = new File("target/test/buffered.file.writer.tst");
        file.getParentFile().mkdirs();
        file.delete();
        return file;
    }
}