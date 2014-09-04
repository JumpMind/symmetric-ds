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

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;

import org.jumpmind.db.platform.DatabaseInfo;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.ISqlReadCursor;
import org.jumpmind.db.sql.ISqlRowMapper;
import org.jumpmind.db.sql.ISqlTemplate;
import org.jumpmind.db.sql.ISqlTransaction;
import org.jumpmind.symmetric.AbstractSymmetricEngine;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.db.AbstractSymmetricDialect;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.io.data.transform.TransformedData;
import org.jumpmind.symmetric.model.Data;
import org.jumpmind.symmetric.model.DataGap;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeChannel;
import org.jumpmind.symmetric.model.ProcessInfo;
import org.jumpmind.symmetric.model.ProcessInfoKey;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.service.IRouterService;
import org.jumpmind.symmetric.service.impl.DataService;
import org.jumpmind.symmetric.service.impl.NodeService;
import org.jumpmind.symmetric.service.impl.ParameterService;
import org.jumpmind.symmetric.service.impl.RouterService;
import org.jumpmind.symmetric.statistic.IStatisticManager;
import org.jumpmind.symmetric.statistic.StatisticManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest({TransformedData.class})
public class DataGapRouteReaderTest {

    final static String ENGINE_NAME = "testengine";
    final static String CHANNEL_ID = "testchannel";
    final static String NODE_ID = "00000";
    final static String NODE_GROUP_ID = "testgroup";
    final static String TABLE1 = "table1";
    final static String TABLE2 = "table2";
    final static String TRAN1 = "1";
    final static String TRAN2 = "2";

    DataGapRouteReader dataGapRouteReader;
    DataService dataService;
    ISqlTemplate sqlTemplate;
    
    @Before
    public void setUp() throws Exception {
        
        sqlTemplate = mock(ISqlTemplate.class);
        dataService = mock(DataService.class);
       
        IParameterService parameterService = mock(ParameterService.class);
        when(parameterService.getInt(ParameterConstants.ROUTING_PEEK_AHEAD_WINDOW)).thenReturn(2);
        when(parameterService.getInt(ParameterConstants.ROUTING_WAIT_FOR_DATA_TIMEOUT_SECONDS)).thenReturn(330);
        when(parameterService.getInt(ParameterConstants.ROUTING_MAX_GAPS_TO_QUALIFY_IN_SQL)).thenReturn(100);
        when(parameterService.getInt(ParameterConstants.ROUTING_DATA_READER_THRESHOLD_GAPS_TO_USE_GREATER_QUERY)).thenReturn(100);
        when(parameterService.is(ParameterConstants.ROUTING_DATA_READER_ORDER_BY_DATA_ID_ENABLED)).thenReturn(true);
        when(parameterService.is(ParameterConstants.SYNCHRONIZE_ALL_JOBS)).thenReturn(true);
        when(parameterService.getEngineName()).thenReturn(ENGINE_NAME);

        IStatisticManager statisticManager = mock(StatisticManager.class);
        when(statisticManager.newProcessInfo((ProcessInfoKey) any())).thenReturn(new ProcessInfo());

        INodeService nodeService = mock(NodeService.class);
        when(nodeService.findIdentity()).thenReturn(new Node(NODE_ID, NODE_GROUP_ID));

        IDatabasePlatform platform = mock(IDatabasePlatform.class);
        when(platform.getSqlTemplate()).thenReturn(sqlTemplate);
        when(platform.getDatabaseInfo()).thenReturn(new DatabaseInfo());

        ISymmetricDialect symmetricDialect = mock(AbstractSymmetricDialect.class);
        when(symmetricDialect.supportsTransactionId()).thenReturn(true);
        when(symmetricDialect.getPlatform()).thenReturn(platform);
        
        ISymmetricEngine engine = mock(AbstractSymmetricEngine.class);
        when(engine.getParameterService()).thenReturn(parameterService);
        when(engine.getStatisticManager()).thenReturn(statisticManager);
        when(engine.getNodeService()).thenReturn(nodeService);
        when(engine.getDataService()).thenReturn(dataService);
        when(engine.getSymmetricDialect()).thenReturn(symmetricDialect);
        IRouterService routerService = new RouterService(engine);
        when(engine.getRouterService()).thenReturn(routerService);

        NodeChannel nodeChannel = new NodeChannel(CHANNEL_ID);
        nodeChannel.setMaxDataToRoute(100);
        nodeChannel.setBatchAlgorithm("default");
        ChannelRouterContext context = new ChannelRouterContext(NODE_ID, nodeChannel,  mock(ISqlTransaction.class));

        dataGapRouteReader = new DataGapRouteReader(context, engine);    
    }

    @After
    public void tearDown() throws Exception {
    }
    
    @Test
    public void myTest() throws Exception {       
        List<DataGap> dataGaps = new ArrayList<DataGap>();
        dataGaps.add(new DataGap(0, 3));
        dataGaps.add(new DataGap(4, Long.MAX_VALUE));

        List<Data> data = new ArrayList<Data>();
        data.add(new Data(1, null, null, null, TABLE1, null, null, null, TRAN1, null));
        data.add(new Data(2, null, null, null, TABLE1, null, null, null, TRAN1, null));
        data.add(new Data(3, null, null, null, TABLE1, null, null, null, TRAN2, null));
        data.add(new Data(4, null, null, null, TABLE1, null, null, null, TRAN1, null));
        data.add(new Data(5, null, null, null, TABLE1, null, null, null, TRAN2, null));        
        
        when(dataService.findDataGaps()).thenReturn(dataGaps);
        when(sqlTemplate.queryForCursor((String) any(), (ISqlRowMapper<Data>) any(), (Object[]) any(),
                (int[]) any())).thenReturn(new ListReadCursor(data));

        dataGapRouteReader.execute();
        
        BlockingQueue<Data> queue = dataGapRouteReader.getDataQueue();
        System.out.println("Queue size: " + queue.size());
        Iterator<Data> iter = queue.iterator();
        while (iter.hasNext()) {
            Data d = iter.next();
            System.out.println("Data: " + d.getDataId() + "," + d.getTableName() + "," + d.getTransactionId());
        }
        
    }
    
    class ListReadCursor implements ISqlReadCursor<Data> {

        Iterator<Data> iterator;
        
        public ListReadCursor(List<Data> list) {
            this.iterator = list.iterator();
        }
        
        public Data next() {
            if (iterator.hasNext()) {
                return iterator.next();
            }
            return null;
        }

        public void close() {
        }        
    }
}
