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
package org.jumpmind.symmetric.route;

import junit.framework.Assert;

import org.jumpmind.symmetric.SyntaxParsingException;
import org.junit.Test;

public class LookupTableDataRouterTest {

    @Test
    public void testValidExpression() {
        LookupTableDataRouter router = new LookupTableDataRouter();
        
        boolean valid = true;
        try {
            router.parse("LOOKUP_TABLE=STORE\r\n" +
                		 "KEY_COLUMN=BRAND_ID\r\n" +
                		 "LOOKUP_KEY_COLUMN=BRAND_ID\r\n" +
                		 "EXTERNAL_ID_COLUMN=STORE_ID");
        } catch(SyntaxParsingException ex) {
            valid = false;
        }
        
        Assert.assertEquals(true, valid);
    }

    @Test
    public void testExpressionWithoutNewLines() {
        LookupTableDataRouter router = new LookupTableDataRouter();
        
        boolean valid = true;
        try {
            router.parse("LOOKUP_TABLE=STORE KEY_COLUMN=BRAND_ID " +
                         "LOOKUP_KEY_COLUMN=BRAND_ID  EXTERNAL_ID_COLUMN=STORE_ID");
        } catch(SyntaxParsingException ex) {
            valid = false;
        }
        
        Assert.assertEquals(false, valid);
    }
    
    @Test
    public void testMissingEqualSign() {
        LookupTableDataRouter router = new LookupTableDataRouter();
        
        boolean valid = true;
        try {
            router.parse("LOOKUP_TABLE=STORE\r\n" +
                         "KEY_COLUMNBRAND_ID\r\n" +         // <-- Missing Equal
                         "LOOKUP_KEY_COLUMN=BRAND_ID\r\n" +
                         "EXTERNAL_ID_COLUMN=STORE_ID");
        } catch(SyntaxParsingException ex) {
            valid = false;
        }
        
        Assert.assertEquals(false, valid);
    }
    
    @Test
    public void testBadKey() {
        LookupTableDataRouter router = new LookupTableDataRouter();
        
        boolean valid = true;
        try {
            router.parse("LOOKUP_TAB=STORE\r\n" +       // <-- Should be LOOKUP_TABLE
                         "KEY_COLUMN=BRAND_ID\r\n" +
                         "LOOKUP_KEY_COLUMN=BRAND_ID\r\n" +
                         "EXTERNAL_ID_COLUMN=STORE_ID");
        } catch(SyntaxParsingException ex) {
            valid = false;
        }
        
        Assert.assertEquals(false, valid);
    }
    
    @Test
    public void testDoubleLine() {
        LookupTableDataRouter router = new LookupTableDataRouter();
        
        boolean valid = true;
        try {
            router.parse("LOOKUP_TABLE=STORE\r\n" +
                         "LOOKUP_TABLE=STORE\r\n" +     // <-- Duplicate
                         "KEY_COLUMN=BRAND_ID\r\n" +
                         "LOOKUP_KEY_COLUMN=BRAND_ID\r\n" +
                         "EXTERNAL_ID_COLUMN=STORE_ID");
        } catch(SyntaxParsingException ex) {
            valid = false;
        }
        
        Assert.assertEquals(false, valid);
    }
    
    @Test
    public void testMissingLine() {
        LookupTableDataRouter router = new LookupTableDataRouter();
        
        boolean valid = true;
        try {
            router.parse("LOOKUP_TABLE=STORE\r\n" +
                         "LOOKUP_KEY_COLUMN=BRAND_ID\r\n" +
                         "EXTERNAL_ID_COLUMN=STORE_ID");
        } catch(SyntaxParsingException ex) {
            valid = false;
        }
        
        Assert.assertEquals(false, valid);
    }
}
