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
package org.jumpmind.symmetric.util;

import java.util.Map;

import junit.framework.Assert;

import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.util.DefaultParameterParser.ParameterMetaData;
import org.junit.Test;

public class DefaultParameterParserTest {

    @Test
    public void testParse() {
        DefaultParameterParser parser = new DefaultParameterParser();
        Map<String, ParameterMetaData> metaData = parser.parse();
        
        Assert.assertNotNull(metaData);
        Assert.assertTrue(metaData.size() > 0);
        ParameterMetaData meta = metaData.get(ParameterConstants.PARAMETER_REFRESH_PERIOD_IN_MS);
        Assert.assertNotNull(meta);
        Assert.assertTrue(meta.getDescription().length() > 0);
        Assert.assertTrue(meta.isDatabaseOverridable());

        meta = metaData.get(ParameterConstants.NODE_GROUP_ID);
        Assert.assertNotNull(meta);
        Assert.assertTrue(meta.getDescription().length() > 0);
        Assert.assertFalse(meta.isDatabaseOverridable());

    
    }
}
