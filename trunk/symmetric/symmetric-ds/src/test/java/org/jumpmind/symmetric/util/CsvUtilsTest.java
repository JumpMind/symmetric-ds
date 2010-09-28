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

import junit.framework.Assert;

import org.junit.Test;

/**
 * @author Chris Henson <chenson42@users.sourceforge.net>
 */
public class CsvUtilsTest {

    @Test
    public void testLastElementIsNull() {
        String[] tokens = CsvUtils.tokenizeCsvData("\"01493931\",\"0\",\"01493931\",,\"UktNQzIxMAD/////AAAABXV1aWQAAAAAEG3jUZmt5UvpiUdFsbLEkJT/////AAAAA2l2AAAAABClDAHak0h0ENSr3PGH8qIU/////wAAAAVjc3VtAAAAACBjkvrLppvDkY1EbaURqm2kpvmcg/j9eUxztrCe4JHXpH0suPSRvgP6LtpJMaH1HZc=\",\"Trevor Lewis\",\"Lewis\",\"Trevorr\",,\"02\",,\"1\",\"16\",,\"0\",,\"0\",\"30683\",\"0\",\"2010-05-18 15:43:21\",\"0\",\"1987-09-24 00:00:00\",\"0\",,\"PS\",\"2009-11-14 21:49:35\",\"2009-11-14 21:49:35\",\"0023\",,\"1\",");
        String expectedNull = tokens[tokens.length-1];
        Assert.assertNull("Expected null.  Instead received: " + expectedNull, expectedNull);
    }
}