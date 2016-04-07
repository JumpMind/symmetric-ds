package org.jumpmind.symmetric.route;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import org.jumpmind.db.platform.DatabaseInfo;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.ISqlTemplate;
import org.jumpmind.db.sql.ISqlTransaction;
import org.jumpmind.symmetric.AbstractSymmetricEngine;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.db.AbstractSymmetricDialect;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.model.DataGap;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.ProcessInfo;
import org.jumpmind.symmetric.model.ProcessInfoKey;
import org.jumpmind.symmetric.service.IContextService;
import org.jumpmind.symmetric.service.IDataService;
import org.jumpmind.symmetric.service.IExtensionService;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.service.IRouterService;
import org.jumpmind.symmetric.service.impl.ContextService;
import org.jumpmind.symmetric.service.impl.DataService;
import org.jumpmind.symmetric.service.impl.ExtensionService;
import org.jumpmind.symmetric.service.impl.NodeService;
import org.jumpmind.symmetric.service.impl.ParameterService;
import org.jumpmind.symmetric.service.impl.RouterService;
import org.jumpmind.symmetric.statistic.IStatisticManager;
import org.jumpmind.symmetric.statistic.StatisticManager;
import org.junit.Before;
import org.junit.Test;

public class DataGapDetectorTest {

    final static String ENGINE_NAME = "testengine";
    final static String CHANNEL_ID = "testchannel";
    final static String NODE_ID = "00000";
    final static String NODE_GROUP_ID = "testgroup";

    ISqlTransaction sqlTransaction;
    IDataService dataService;
    IParameterService parameterService;
    IContextService contextService;
    ISymmetricDialect symmetricDialect;
    IRouterService routerService;
    IStatisticManager statisticManager;
    INodeService nodeService;

    @Before
    public void setUp() throws Exception {
        ISqlTemplate sqlTemplate = mock(ISqlTemplate.class);
        sqlTransaction = mock(ISqlTransaction.class); 
        when(sqlTemplate.startSqlTransaction()).thenReturn(sqlTransaction);
        IDatabasePlatform platform = mock(IDatabasePlatform.class);
        when(platform.getDatabaseInfo()).thenReturn(new DatabaseInfo());
        when(platform.getSqlTemplate()).thenReturn(sqlTemplate);
        symmetricDialect = mock(AbstractSymmetricDialect.class);
        when(symmetricDialect.getPlatform()).thenReturn(platform);
        when(symmetricDialect.supportsTransactionViews()).thenReturn(false);
        when(symmetricDialect.getDatabaseTime()).thenReturn(0L);

        parameterService = mock(ParameterService.class);
        when(parameterService.getEngineName()).thenReturn(ENGINE_NAME);
        when(parameterService.getLong(ParameterConstants.ROUTING_STALE_DATA_ID_GAP_TIME)).thenReturn(60000L);
        when(parameterService.getInt(ParameterConstants.DATA_ID_INCREMENT_BY)).thenReturn(1);
        when(parameterService.getInt(ParameterConstants.ROUTING_LARGEST_GAP_SIZE)).thenReturn(50000000);
        when(parameterService.getLong(ParameterConstants.DBDIALECT_ORACLE_TRANSACTION_VIEW_CLOCK_SYNC_THRESHOLD_MS)).thenReturn(60000L);        

        IExtensionService extensionService = mock(ExtensionService.class);
        ISymmetricEngine engine = mock(AbstractSymmetricEngine.class);
        when(engine.getParameterService()).thenReturn(parameterService);
        when(engine.getStatisticManager()).thenReturn(statisticManager);
        when(engine.getNodeService()).thenReturn(nodeService);
        when(engine.getDataService()).thenReturn(dataService);
        when(engine.getSymmetricDialect()).thenReturn(symmetricDialect);
        when(engine.getExtensionService()).thenReturn(extensionService);
        routerService = new RouterService(engine);
        when(engine.getRouterService()).thenReturn(routerService);

        contextService = mock(ContextService.class);
        dataService = mock(DataService.class);
        
        statisticManager = mock(StatisticManager.class);
        when(statisticManager.newProcessInfo((ProcessInfoKey) any())).thenReturn(new ProcessInfo());
        
        nodeService = mock(NodeService.class);
        when(nodeService.findIdentity()).thenReturn(new Node(NODE_ID, NODE_GROUP_ID));
    }

    protected DataGapFastDetector newGapDetector() {
        return new DataGapFastDetector(dataService, parameterService, contextService, symmetricDialect, routerService, statisticManager, nodeService);
    }

    protected void runGapDetector(List<DataGap> dataGaps, List<Long> dataIds, boolean isAllDataRead) {
        when(dataService.findDataGaps()).thenReturn(dataGaps);
        DataGapFastDetector detector = newGapDetector();
        detector.beforeRouting();
        detector.addDataIds(dataIds);
        detector.setIsAllDataRead(true);
        detector.afterRouting();
    }
    
    @Test
    public void testNewGap() throws Exception {
        List<DataGap> dataGaps = new ArrayList<DataGap>();
        dataGaps.add(new DataGap(3, 3));
        dataGaps.add(new DataGap(4, 50000004));
        
        List<Long> dataIds = new ArrayList<Long>();
        dataIds.add(100L);

        runGapDetector(dataGaps, dataIds, true);

        verify(dataService).findDataGaps();
        verify(dataService).deleteDataGap(sqlTransaction, new DataGap(4, 50000004));
        verify(dataService).insertDataGap(sqlTransaction, new DataGap(4, 99));        
        verify(dataService).insertDataGap(new DataGap(101, 50000100));
        verifyNoMoreInteractions(dataService);
    }
}
