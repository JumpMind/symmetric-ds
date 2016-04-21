package org.jumpmind.symmetric.service.impl;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import org.jumpmind.db.platform.DatabaseInfo;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.ISqlRowMapper;
import org.jumpmind.db.sql.ISqlTemplate;
import org.jumpmind.db.sql.ISqlTransaction;
import org.jumpmind.symmetric.AbstractSymmetricEngine;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.db.AbstractSymmetricDialect;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.model.DataGap;
import org.jumpmind.symmetric.service.IDataService;
import org.jumpmind.symmetric.service.IExtensionService;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.util.AppUtils;
import org.junit.Before;
import org.junit.Test;
import org.mockito.AdditionalMatchers;
import org.mockito.Matchers;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class DataServiceTest {

    ISqlTemplate sqlTemplate;
    ISqlTransaction sqlTransaction;
    IDataService dataService;
    IParameterService parameterService;
    ISymmetricDialect symmetricDialect;

    @Before
    public void setUp() throws Exception {
        sqlTemplate = mock(ISqlTemplate.class);
        sqlTransaction = mock(ISqlTransaction.class); 
        when(sqlTemplate.startSqlTransaction()).thenReturn(sqlTransaction);
        IDatabasePlatform platform = mock(IDatabasePlatform.class);
        when(platform.getDatabaseInfo()).thenReturn(new DatabaseInfo());
        when(platform.getSqlTemplate()).thenReturn(sqlTemplate);
        symmetricDialect = mock(AbstractSymmetricDialect.class);
        when(symmetricDialect.getPlatform()).thenReturn(platform);

        parameterService = mock(ParameterService.class);
        when(parameterService.getLong(ParameterConstants.ROUTING_LARGEST_GAP_SIZE)).thenReturn(50000000L);

        IExtensionService extensionService = mock(ExtensionService.class);
        ISymmetricEngine engine = mock(AbstractSymmetricEngine.class);
        when(engine.getParameterService()).thenReturn(parameterService);
        when(engine.getSymmetricDialect()).thenReturn(symmetricDialect);

        dataService = new DataService(engine, extensionService);
    }

    @Test
    public void testFindDataGaps() throws Exception {
        final List<DataGap> gaps1 = new ArrayList<DataGap>();
        
        final List<DataGap> gaps2 = new ArrayList<DataGap>();
        gaps2.add(new DataGap(0, 50000000));
        
        when(sqlTemplate.queryForLong(Matchers.anyString())).thenReturn(0L);
        @SuppressWarnings("unchecked")
        ISqlRowMapper<DataGap> anyMapper = (ISqlRowMapper<DataGap>) Matchers.anyObject();
        
        when(sqlTemplate.query(Matchers.anyString(), anyMapper, Matchers.anyVararg())).thenAnswer(new Answer<List<DataGap>>() {
            int i;
            public List<DataGap> answer(InvocationOnMock invocation) {
                return i++ == 0 ? gaps1 : gaps2;
            }
        });
        dataService.findDataGaps();

        verify(sqlTransaction).prepareAndExecute(Matchers.anyString(),
                AdditionalMatchers.aryEq(new Object[] { DataGap.Status.GP.name(), AppUtils.getHostName(), 0L, 50000000L }),
                AdditionalMatchers.aryEq(new int[] { 12, 12, 2, 2 }));

        verify(sqlTransaction).commit();
        verify(sqlTransaction).close();
        verifyNoMoreInteractions(sqlTransaction);
    }
    
    @Test
    public void testFindDataGaps2() throws Exception {
        final List<DataGap> gaps1 = new ArrayList<DataGap>();
        gaps1.add(new DataGap(30953884, 80953883));
        gaps1.add(new DataGap(30953883, 80953883));
        
        when(sqlTemplate.queryForLong(Matchers.anyString())).thenReturn(0L);
        @SuppressWarnings("unchecked")
        ISqlRowMapper<DataGap> anyMapper = (ISqlRowMapper<DataGap>) Matchers.anyObject();
        when(sqlTemplate.query(Matchers.anyString(), anyMapper, Matchers.anyVararg())).thenReturn(gaps1);

        dataService.findDataGaps();

        verifyNoMoreInteractions(sqlTransaction);
    }

}
