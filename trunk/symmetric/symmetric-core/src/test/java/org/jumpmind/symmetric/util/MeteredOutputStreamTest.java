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

import java.io.IOException;
import java.util.Random;

import org.junit.Test;

/**
 * 
 */
public class MeteredOutputStreamTest {

    @Test
    public static void testBasic() throws IOException {
        final long rate = 5 * 1024;
        final long count = 20;
        final int bufferSize = 8192;

        MeteredOutputStream out = new MeteredOutputStream(new NullOutputStream(), rate, 8192, 1);
        long start = System.currentTimeMillis();
        byte[] testBytes = new byte[bufferSize];

        Random r = new Random();

        r.nextBytes(testBytes);

        long i = 0;
        for (i = 0; i < count; i++) {
            out.write(testBytes, 0, testBytes.length);

            if ((i % 10) == 0) {
                System.out.print('#');
            }
        }

        double expectedTime = (bufferSize * count) / rate;
        double actualTime = (System.currentTimeMillis() - start + 1) / 1000;
        assert (actualTime >= expectedTime - 2 && actualTime <= expectedTime + 2);
    }
}