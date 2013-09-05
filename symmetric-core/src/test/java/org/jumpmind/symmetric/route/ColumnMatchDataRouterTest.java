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

import java.util.List;

import static org.junit.Assert.*;

import org.jumpmind.symmetric.route.ColumnMatchDataRouter.Expression;
import org.junit.Test;

public class ColumnMatchDataRouterTest {

    @Test
    public void testExpressionUsingLineFeedsParsing() {
        ColumnMatchDataRouter router = new ColumnMatchDataRouter();
        List<Expression> expressions = router.parse("one=two\ntwo=three\rthree!=:EXTERNAL_ID");
        assertEquals(3, expressions.size());
        assertEquals("two",expressions.get(0).tokens[1]);
        assertEquals("three",expressions.get(2).tokens[0]);
        assertEquals(false,expressions.get(2).equals);
    }
    
    @Test
    public void testExpressionOrParsing() {
        ColumnMatchDataRouter router = new ColumnMatchDataRouter();
        List<Expression> expressions = router.parse("one=door OR two=three or three!=:EXTERNAL_ID");
        assertEquals(3, expressions.size());
        assertEquals("door",expressions.get(0).tokens[1]);
        assertEquals("three",expressions.get(2).tokens[0]);
        assertEquals(false,expressions.get(2).equals);
    }
    
    @Test
    public void testExpressionTickParsing() {
        ColumnMatchDataRouter router = new ColumnMatchDataRouter();
        List<Expression> expressions = router.parse("one='two three' OR four='five'\r\nor six=isn't \r\n seven='can''t'" +
                                                    " or eight='yall \n nine=' ten  ' or eleven  =  'twelve'  ");
        assertEquals(7, expressions.size());

        assertEquals("one",expressions.get(0).tokens[0]);
        assertEquals("two three",expressions.get(0).tokens[1]);

        assertEquals("four",expressions.get(1).tokens[0]);
        assertEquals("five",expressions.get(1).tokens[1]);

        assertEquals("six",expressions.get(2).tokens[0]);
        assertEquals("isn't",expressions.get(2).tokens[1]);

        assertEquals("seven",expressions.get(3).tokens[0]);
        assertEquals("can't",expressions.get(3).tokens[1]);

        assertEquals("eight",expressions.get(4).tokens[0]);
        assertEquals("'yall",expressions.get(4).tokens[1]);

        assertEquals("nine",expressions.get(5).tokens[0]);
        assertEquals(" ten  ",expressions.get(5).tokens[1]);

        assertEquals("eleven",expressions.get(6).tokens[0]);
        assertEquals("twelve",expressions.get(6).tokens[1]);


    }
    
    @Test
    public void testExpressionOrAndLineFeedsParsing() {
        ColumnMatchDataRouter router = new ColumnMatchDataRouter();
        List<Expression> expressions = router.parse("one=two OR three=four\r\nor   five!=:EXTERNAL_ID");
        assertEquals(3, expressions.size());
        assertEquals("two",expressions.get(0).tokens[1]);
        assertEquals("three",expressions.get(1).tokens[0]);
        assertEquals("five",expressions.get(2).tokens[0]);
        assertEquals(false,expressions.get(2).equals);
    }
    
    @Test
    public void testExpressionWithOrInColumnName() {
        ColumnMatchDataRouter router = new ColumnMatchDataRouter();
        List<Expression> expressions = router.parse("ORDER_ID=:EXTERNAL_ID");
        assertEquals(1, expressions.size());
        assertEquals("ORDER_ID",expressions.get(0).tokens[0]);
        assertEquals(":EXTERNAL_ID",expressions.get(0).tokens[1]);
    }
    
}
