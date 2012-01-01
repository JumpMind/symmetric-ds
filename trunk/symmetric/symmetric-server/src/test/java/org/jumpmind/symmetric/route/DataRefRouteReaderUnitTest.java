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
package org.jumpmind.symmetric.route;

import org.jumpmind.symmetric.common.logging.ILog;
import org.jumpmind.symmetric.common.logging.LogFactory;
import org.jumpmind.symmetric.model.Channel;
import org.jumpmind.symmetric.service.ISqlProvider;
import org.jumpmind.symmetric.service.impl.RouterServiceSqlMap;
import org.junit.Assert;
import org.junit.Test;

public class DataRefRouteReaderUnitTest {

    protected ILog log = LogFactory.getLog(getClass());

    private static final String BLANK = "''";

    RouterServiceSqlMap sql = new RouterServiceSqlMap(null, null);

    @Test
    public void testOldDataReplacement() {
        DataRefRouteReader reader = new DataRefRouteReader(log, getSqlProvider(), null, null);
        Channel channel = new Channel();
        Assert.assertTrue(reader.getSql(DataRefRouteReader.SELECT_DATA_TO_BATCH_SQL, channel)
                .contains("old_data"));
        Assert.assertFalse(reader.getSql(DataRefRouteReader.SELECT_DATA_TO_BATCH_SQL, channel)
                .contains(BLANK));
        channel.setUseOldDataToRoute(false);
        Assert.assertFalse(reader.getSql(DataRefRouteReader.SELECT_DATA_TO_BATCH_SQL, channel)
                .contains("old_data"));
        Assert.assertTrue(reader.getSql(DataRefRouteReader.SELECT_DATA_TO_BATCH_SQL, channel)
                .contains(BLANK));
    }

    @Test
    public void testRowDataReplacement() {
        DataRefRouteReader reader = new DataRefRouteReader(log, getSqlProvider(), null, null);
        Channel channel = new Channel();
        Assert.assertTrue(reader.getSql(DataRefRouteReader.SELECT_DATA_TO_BATCH_SQL, channel)
                .contains("row_data"));
        Assert.assertFalse(reader.getSql(DataRefRouteReader.SELECT_DATA_TO_BATCH_SQL, channel)
                .contains(BLANK));
        channel.setUseRowDataToRoute(false);
        Assert.assertFalse(reader.getSql(DataRefRouteReader.SELECT_DATA_TO_BATCH_SQL, channel)
                .contains("row_data"));
        Assert.assertTrue(reader.getSql(DataRefRouteReader.SELECT_DATA_TO_BATCH_SQL, channel)
                .contains(BLANK));
    }

    @Test
    public void testOldAndRowDataReplacement() {
        DataRefRouteReader reader = new DataRefRouteReader(log, getSqlProvider(), null, null);
        Channel channel = new Channel();
        Assert.assertTrue(reader.getSql(DataRefRouteReader.SELECT_DATA_TO_BATCH_SQL, channel)
                .contains("row_data"));
        Assert.assertTrue(reader.getSql(DataRefRouteReader.SELECT_DATA_TO_BATCH_SQL, channel)
                .contains("old_data"));
        Assert.assertFalse(reader.getSql(DataRefRouteReader.SELECT_DATA_TO_BATCH_SQL, channel)
                .contains(BLANK));
        channel.setUseRowDataToRoute(false);
        channel.setUseOldDataToRoute(false);
        Assert.assertFalse(reader.getSql(DataRefRouteReader.SELECT_DATA_TO_BATCH_SQL, channel)
                .contains("row_data"));
        Assert.assertFalse(reader.getSql(DataRefRouteReader.SELECT_DATA_TO_BATCH_SQL, channel)
                .contains("old_data"));
        Assert.assertTrue(reader.getSql(DataRefRouteReader.SELECT_DATA_TO_BATCH_SQL, channel)
                .contains(BLANK));
    }

    private ISqlProvider getSqlProvider() {
        return new ISqlProvider() {
            public String getSql(String... keys) {
                return sql.getSql(keys[0]);
            }
        };
    }

}