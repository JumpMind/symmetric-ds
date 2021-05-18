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
package org.jumpmind.symmetric.service.impl;

import static org.mockito.Mockito.mock;
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
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;

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
    public void testFindDataGaps2() throws Exception {
        final List<DataGap> gaps1 = new ArrayList<DataGap>();
        gaps1.add(new DataGap(30953884, 80953883));
        gaps1.add(new DataGap(30953883, 80953883));
        
        when(sqlTemplate.queryForLong(ArgumentMatchers.anyString())).thenReturn(0L);
        String sql = ArgumentMatchers.anyString();
        @SuppressWarnings("unchecked")
        ISqlRowMapper<DataGap> anyMapper = (ISqlRowMapper<DataGap>) ArgumentMatchers.any();
        when(sqlTemplate.query(sql, anyMapper, (Object[])ArgumentMatchers.any())).thenReturn(gaps1);

        dataService.findDataGaps();

        verifyNoMoreInteractions(sqlTransaction);
    }

}
