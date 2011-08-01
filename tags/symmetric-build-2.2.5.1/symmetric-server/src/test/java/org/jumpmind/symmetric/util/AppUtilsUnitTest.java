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

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.time.DateUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * 
 */
public class AppUtilsUnitTest {

    @Test
    public void testGetLocalDateForOffset() {
        Date gmt = AppUtils.getLocalDateForOffset("+00:00");
        Date plusFour = AppUtils.getLocalDateForOffset("+04:00");
        Date minusFour = AppUtils.getLocalDateForOffset("-04:00");
        long nearZero = plusFour.getTime() - gmt.getTime() - DateUtils.MILLIS_PER_HOUR * 4;
        Assert.assertTrue(nearZero + " was the left over ms",  Math.abs(nearZero) < 1000);
        nearZero = plusFour.getTime() - minusFour.getTime() - DateUtils.MILLIS_PER_HOUR * 8;
        Assert.assertTrue(nearZero + " was the left over ms", Math.abs(nearZero) < 1000);
    }
    
    @Test
    public void testReplaceTokens() {
        Assert.assertEquals("test", AppUtils.replaceTokens("test", null, true));
        Assert.assertEquals("test", AppUtils.replaceTokens("test", new HashMap<String, String>(), true));
        Map<String, String> params = new HashMap<String, String>();
        params.put("test", "1");
        Assert.assertEquals("test1", AppUtils.replaceTokens("test$(test)", params, true));
        Assert.assertEquals("test0001", AppUtils.replaceTokens("test$(test|%04d)", params, true));
    }
    
    @Test
    public void testReplaceCurrentTimestamp() {
        String beforeSql = "insert into sym_node values ('00000', 'test-root-group', '00000', 1, null, null, '2.0', null, null, current_timestamp, null, 0, 0, '00000', 'engine')";
        String afterSql = "insert into sym_node values ('00000', 'test-root-group', '00000', 1, null, null, '2.0', null, null, XXXX, null, 0, 0, '00000', 'engine')";
        Map<String,String> replacementTokens = new HashMap<String, String>();
        replacementTokens.put("current_timestamp", "XXXX");
        Assert.assertEquals(afterSql, AppUtils.replaceTokens(beforeSql, replacementTokens, false));
        
    }
    
}