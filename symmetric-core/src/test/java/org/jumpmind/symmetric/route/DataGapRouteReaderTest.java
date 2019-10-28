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

import static org.junit.Assert.assertEquals;
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
import org.jumpmind.symmetric.service.IExtensionService;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.service.IRouterService;
import org.jumpmind.symmetric.service.impl.DataService;
import org.jumpmind.symmetric.service.impl.ExtensionService;
import org.jumpmind.symmetric.service.impl.NodeService;
import org.jumpmind.symmetric.service.impl.ParameterService;
import org.jumpmind.symmetric.service.impl.RouterService;
import org.jumpmind.symmetric.statistic.IStatisticManager;
import org.jumpmind.symmetric.statistic.StatisticManager;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ TransformedData.class })
public class DataGapRouteReaderTest {

    final static String ENGINE_NAME = "testengine";
    final static String CHANNEL_ID = "testchannel";
    final static String NODE_ID = "00000";
    final static String NODE_GROUP_ID = "testgroup";
    final static String TABLE1 = "table1";
    final static String TABLE2 = "table2";
    final static String TRAN1 = "1";
    final static String TRAN2 = "2";

    DataService dataService;
    ISqlTemplate sqlTemplate;
    IParameterService parameterService;
    NodeChannel nodeChannel;

    @Before
    public void setUp() throws Exception {
        sqlTemplate = mock(ISqlTemplate.class);
        dataService = mock(DataService.class);
        parameterService = mock(ParameterService.class);
        nodeChannel = new NodeChannel(CHANNEL_ID);
        nodeChannel.setMaxDataToRoute(100);
        nodeChannel.setBatchAlgorithm(DefaultBatchAlgorithm.NAME);
    }

    protected DataGapRouteReader buildReader(int peekAheadMemoryThreshold, List<DataGap> dataGaps) throws Exception {

        when(parameterService.getEngineName()).thenReturn(ENGINE_NAME);
        when(parameterService.is(ParameterConstants.SYNCHRONIZE_ALL_JOBS)).thenReturn(true);
        when(parameterService.getInt(ParameterConstants.ROUTING_WAIT_FOR_DATA_TIMEOUT_SECONDS))
                .thenReturn(330);
        when(parameterService.getInt(ParameterConstants.ROUTING_PEEK_AHEAD_MEMORY_THRESHOLD))
                .thenReturn(peekAheadMemoryThreshold);
        when(parameterService.getInt(ParameterConstants.ROUTING_MAX_GAPS_TO_QUALIFY_IN_SQL))
                .thenReturn(100);
        when(
                parameterService
                        .getInt(ParameterConstants.ROUTING_DATA_READER_THRESHOLD_GAPS_TO_USE_GREATER_QUERY))
                .thenReturn(100);
        when(parameterService.is(ParameterConstants.ROUTING_DATA_READER_ORDER_BY_DATA_ID_ENABLED))
                .thenReturn(true);

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

        IExtensionService extensionService = mock(ExtensionService.class);

        ISymmetricEngine engine = mock(AbstractSymmetricEngine.class);
        when(engine.getParameterService()).thenReturn(parameterService);
        when(engine.getStatisticManager()).thenReturn(statisticManager);
        when(engine.getNodeService()).thenReturn(nodeService);
        when(engine.getDataService()).thenReturn(dataService);
        when(engine.getSymmetricDialect()).thenReturn(symmetricDialect);
        when(engine.getExtensionService()).thenReturn(extensionService);
        IRouterService routerService = new RouterService(engine);
        when(engine.getRouterService()).thenReturn(routerService);

        ChannelRouterContext context = new ChannelRouterContext(NODE_ID, nodeChannel,
                mock(ISqlTransaction.class));
        context.setDataGaps(dataGaps);

        return new DataGapRouteReader(context, engine);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testTransactionalOrderingWithGaps() throws Exception {

        nodeChannel.setBatchAlgorithm(DefaultBatchAlgorithm.NAME);
        nodeChannel.setMaxDataToRoute(100);

        when(parameterService.getInt(ParameterConstants.ROUTING_PEEK_AHEAD_WINDOW)).thenReturn(2);

        List<DataGap> dataGaps = new ArrayList<DataGap>();
        dataGaps.add(new DataGap(0, 3));
        dataGaps.add(new DataGap(4, Long.MAX_VALUE));

        List<Data> data = new ArrayList<Data>();
        data.add(new Data(1, null, null, null, TABLE1, null, null, null, TRAN1, null));
        data.add(new Data(2, null, null, null, TABLE1, null, null, null, TRAN1, null));
        data.add(new Data(3, null, null, null, TABLE1, null, null, null, TRAN2, null));
        data.add(new Data(4, null, null, null, TABLE1, null, null, null, TRAN1, null));
        data.add(new Data(5, null, null, null, TABLE1, null, null, null, TRAN2, null));

        when(sqlTemplate.queryForCursor((String) any(), (ISqlRowMapper<Data>) any(),
            (Object[]) any(), (int[]) any())).thenReturn(new ListReadCursor(data));

        DataGapRouteReader dataGapRouteReader = buildReader(50, dataGaps);
        dataGapRouteReader.execute();

        BlockingQueue<Data> queue = dataGapRouteReader.getDataQueue();
        assertEquals(6, queue.size());
        Iterator<Data> iter = queue.iterator();
        int index = 0;
        long ids[] = { 1, 2, 4, 3, 5, -1 };
        while (iter.hasNext()) {
            Data d = iter.next();
            assertEquals(ids[index], d.getDataId());
            index++;
        }

    }

    @Test
    public void testTransactionalChannelMaxDataToRoute() throws Exception {

        nodeChannel.setBatchAlgorithm(TransactionalBatchAlgorithm.NAME);
        nodeChannel.setMaxDataToRoute(3);

        when(parameterService.getInt(ParameterConstants.ROUTING_PEEK_AHEAD_WINDOW)).thenReturn(100);

        List<DataGap> dataGaps = new ArrayList<DataGap>();
        dataGaps.add(new DataGap(0, Long.MAX_VALUE));

        List<Data> data = new ArrayList<Data>();
        data.add(new Data(1, null, null, null, TABLE1, null, null, null, TRAN1, null));
        data.add(new Data(2, null, null, null, TABLE1, null, null, null, TRAN1, null));
        data.add(new Data(3, null, null, null, TABLE1, null, null, null, TRAN2, null));
        data.add(new Data(4, null, null, null, TABLE1, null, null, null, TRAN2, null));
        data.add(new Data(5, null, null, null, TABLE1, null, null, null, TRAN1, null));
        data.add(new Data(6, null, null, null, TABLE1, null, null, null, TRAN1, null));

        ISqlRowMapper<Data> mapper = any();
        when(sqlTemplate.queryForCursor((String) any(), mapper, (Object[]) any(), (int[]) any()))
                .thenReturn(new ListReadCursor(data));

        DataGapRouteReader dataGapRouteReader = buildReader(50, dataGaps);
        dataGapRouteReader.execute();

        BlockingQueue<Data> queue = dataGapRouteReader.getDataQueue();
        assertEquals(5, queue.size());
        Iterator<Data> iter = queue.iterator();
        int index = 0;
        long ids[] = { 1, 2, 5, 6, -1 };
        while (iter.hasNext()) {
            Data d = iter.next();
            assertEquals(ids[index], d.getDataId());
            index++;
        }

    }

    @Test
    public void testTransactionalChannelTwoTransactionsRouted() throws Exception {

        nodeChannel.setBatchAlgorithm(TransactionalBatchAlgorithm.NAME);
        nodeChannel.setMaxDataToRoute(100);

        when(parameterService.getInt(ParameterConstants.ROUTING_PEEK_AHEAD_WINDOW)).thenReturn(100);

        List<DataGap> dataGaps = new ArrayList<DataGap>();
        dataGaps.add(new DataGap(0, Long.MAX_VALUE));

        List<Data> data = new ArrayList<Data>();
        data.add(new Data(1, null, null, null, TABLE1, null, null, null, TRAN1, null));
        data.add(new Data(2, null, null, null, TABLE1, null, null, null, TRAN1, null));
        data.add(new Data(3, null, null, null, TABLE1, null, null, null, TRAN2, null));
        data.add(new Data(4, null, null, null, TABLE1, null, null, null, TRAN2, null));
        data.add(new Data(5, null, null, null, TABLE1, null, null, null, TRAN1, null));
        data.add(new Data(6, null, null, null, TABLE1, null, null, null, TRAN1, null));

        ISqlRowMapper<Data> mapper = any();
        when(sqlTemplate.queryForCursor((String) any(), mapper, (Object[]) any(), (int[]) any()))
                .thenReturn(new ListReadCursor(data));

        DataGapRouteReader dataGapRouteReader = buildReader(50, dataGaps);
        dataGapRouteReader.execute();

        BlockingQueue<Data> queue = dataGapRouteReader.getDataQueue();
        assertEquals(7, queue.size());
        Iterator<Data> iter = queue.iterator();
        int index = 0;
        long ids[] = { 1, 2, 5, 6, 3, 4, -1 };
        while (iter.hasNext()) {
            Data d = iter.next();
            assertEquals(ids[index], d.getDataId());
            index++;
        }

    }

    @Test
    public void testTransactionalChannelReachMaxPeekAheadSizeThreshold() throws Exception {

        nodeChannel.setBatchAlgorithm(TransactionalBatchAlgorithm.NAME);
        nodeChannel.setMaxDataToRoute(100);

        when(parameterService.getInt(ParameterConstants.ROUTING_PEEK_AHEAD_WINDOW)).thenReturn(100);

        List<DataGap> dataGaps = new ArrayList<DataGap>();
        dataGaps.add(new DataGap(0, Long.MAX_VALUE));

        List<Data> data = new ArrayList<Data>();
        data.add(new Data(1, null, null, null, TABLE1, null, null, null, TRAN1, null));
        data.add(new Data(2, null, null, null, TABLE1, null, null, null, TRAN1, null));
        data.add(new Data(3, null, null, null, TABLE1, null, null, null, TRAN2, null));
        data.add(new Data(4, null, null, null, TABLE1, null, null, null, TRAN1, null));
        data.add(new Data(5, null, null, null, TABLE1, null, null, null, TRAN2, null));
        data.add(new Data(6, null, null, null, TABLE1, null, null, null, TRAN1, null));
        data.add(new Data(7, null, null, null, TABLE1, null, null, null, TRAN2, null));

        ISqlRowMapper<Data> mapper = any();

        when(sqlTemplate.queryForCursor((String) any(), mapper, (Object[]) any(), (int[]) any()))
                .thenReturn(new ListReadCursor(data));

        DataGapRouteReader dataGapRouteReader = buildReader(0, dataGaps);
        dataGapRouteReader.execute();

        BlockingQueue<Data> queue = dataGapRouteReader.getDataQueue();
        assertEquals(5, queue.size());
        Iterator<Data> iter = queue.iterator();
        int index = 0;
        long ids[] = { 1, 2, 4, 6, -1 };
        while (iter.hasNext()) {
            Data d = iter.next();
            assertEquals(ids[index], d.getDataId());
            index++;
        }
    }

    @Test
    public void testDontPeekAheadWhenPeekAheadQueueIsAlreadyFull() throws Exception {

        nodeChannel.setBatchAlgorithm(TransactionalBatchAlgorithm.NAME);
        nodeChannel.setMaxDataToRoute(100);

        when(parameterService.getInt(ParameterConstants.ROUTING_PEEK_AHEAD_WINDOW)).thenReturn(5);

        List<DataGap> dataGaps = new ArrayList<DataGap>();
        dataGaps.add(new DataGap(0, Long.MAX_VALUE));

        List<Data> data = new ArrayList<Data>();
        for (int i = 0; i < 100; i++) {
            data.add(new Data(i, null, null, null, TABLE1, null, null, null, Integer.toString(i%2 == 0 ? i : i-1),
                    null));
        }

        ISqlRowMapper<Data> mapper = any();

        when(sqlTemplate.queryForCursor((String) any(), mapper, (Object[]) any(), (int[]) any()))
                .thenReturn(new ListReadCursor(data));

        DataGapRouteReader dataGapRouteReader = buildReader(100, dataGaps);
        dataGapRouteReader.execute();
        
        /*
         * Test that the peek ahead queue size doesn't get bigger than twice the peek ahead size
         */
        assertEquals(10,dataGapRouteReader.context.getMaxPeekAheadQueueSize());
        assertEquals(21,dataGapRouteReader.context.getPeekAheadFillCount());

        BlockingQueue<Data> queue = dataGapRouteReader.getDataQueue();
        assertEquals(101, queue.size());
        Iterator<Data> iter = queue.iterator();
        int index = 0;
        while (iter.hasNext()) {
            Data d = iter.next();
            assertEquals(index < 100 ? index : -1, d.getDataId());
            index++;
        }

    }

    @Test
    public void testNonTransactionalChannelMaxDataToRoute() throws Exception {

        nodeChannel.setBatchAlgorithm(NonTransactionalBatchAlgorithm.NAME);
        nodeChannel.setMaxDataToRoute(3);

        when(parameterService.getInt(ParameterConstants.ROUTING_PEEK_AHEAD_WINDOW)).thenReturn(2);

        List<DataGap> dataGaps = new ArrayList<DataGap>();
        dataGaps.add(new DataGap(0, Long.MAX_VALUE));

        List<Data> data = new ArrayList<Data>();
        data.add(new Data(1, null, null, null, TABLE1, null, null, null, TRAN1, null));
        data.add(new Data(2, null, null, null, TABLE1, null, null, null, TRAN1, null));
        data.add(new Data(3, null, null, null, TABLE1, null, null, null, TRAN2, null));
        data.add(new Data(4, null, null, null, TABLE1, null, null, null, TRAN1, null));
        data.add(new Data(5, null, null, null, TABLE1, null, null, null, TRAN2, null));

        ISqlRowMapper<Data> mapper = any();
        when(sqlTemplate.queryForCursor((String) any(), mapper, (Object[]) any(), (int[]) any()))
                .thenReturn(new ListReadCursor(data));

        DataGapRouteReader dataGapRouteReader = buildReader(50, dataGaps);
        dataGapRouteReader.execute();

        BlockingQueue<Data> queue = dataGapRouteReader.getDataQueue();
        assertEquals(4, queue.size());
        Iterator<Data> iter = queue.iterator();
        int index = 0;
        long ids[] = { 1, 2, 4, -1 };
        while (iter.hasNext()) {
            Data d = iter.next();
            assertEquals(ids[index], d.getDataId());
            index++;
        }

    }

    static class ListReadCursor implements ISqlReadCursor<Data> {

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
