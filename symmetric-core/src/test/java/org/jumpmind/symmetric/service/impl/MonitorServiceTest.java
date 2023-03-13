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
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.ISqlRowMapper;
import org.jumpmind.db.sql.ISqlTemplate;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.cache.ICacheManager;
import org.jumpmind.symmetric.common.ContextConstants;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.model.Monitor;
import org.jumpmind.symmetric.model.MonitorEvent;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.monitor.IMonitorType;
import org.jumpmind.symmetric.monitor.MonitorTypeLog;
import org.jumpmind.symmetric.service.IClusterService;
import org.jumpmind.symmetric.service.IContextService;
import org.jumpmind.symmetric.service.IExtensionService;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IParameterService;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

class MonitorServiceTest {
    ISymmetricEngine engine = mock(ISymmetricEngine.class);
    ISymmetricDialect symmetricDialect = mock(ISymmetricDialect.class);
    IDatabasePlatform databasePlatform = mock(IDatabasePlatform.class);
    IParameterService parameterService = mock(IParameterService.class);
    IExtensionService extensionService = mock(IExtensionService.class);
    IContextService contextService = mock(IContextService.class);
    IClusterService clusterService = mock(IClusterService.class);
    INodeService nodeService = mock(INodeService.class);
    ICacheManager cacheManager = mock(ICacheManager.class);

    @Test
    void testMonitorService() throws Exception {
        Map<String, String> testMap = new HashMap<String, String>();
        when(databasePlatform.getName()).thenReturn("Postgres");
        when(engine.getParameterService()).thenReturn(parameterService);
        when(symmetricDialect.getPlatform()).thenReturn(databasePlatform);
        when(symmetricDialect.getSqlReplacementTokens()).thenReturn(testMap);
        when(engine.getExtensionService()).thenReturn(extensionService);
        MonitorService testMonitorService = new MonitorService(engine, symmetricDialect);
        // not quite sure what exactly to test here.
        // the uncovered lines are for loops that use
        // extensionService.addExtensionPoint(...)
        // doesn't really have to do with monitor service?
    }

    @Test
    void testMonitorServiceUpdate() throws Exception {
        INodeService nodeService = mock(INodeService.class);
        ICacheManager cacheManager = mock(ICacheManager.class);
        Node identity = mock(Node.class);
        ISqlTemplate sqlTemplate = mock(ISqlTemplate.class);
        MonitorEvent monitorEvent = mock(MonitorEvent.class);
        Monitor monitor = mock(Monitor.class);
        MonitorTypeLog monitorType = mock(MonitorTypeLog.class);
        List<MonitorEvent> list = new ArrayList<MonitorEvent>();
        String sql = ArgumentMatchers.anyString();
        @SuppressWarnings("unchecked")
        ISqlRowMapper<MonitorEvent> mapper = (ISqlRowMapper<MonitorEvent>) ArgumentMatchers.any();
        when(sqlTemplate.query(sql, mapper, (Object[]) ArgumentMatchers.any())).thenAnswer(new Answer<List<MonitorEvent>>() {
            public List<MonitorEvent> answer(InvocationOnMock invocation) {
                list.add(monitorEvent);
                return list;
            }
        });
        List<Monitor> activeMonitors = new ArrayList<Monitor>();
        activeMonitors.add(monitor);
        Map<String, String> testMap = new HashMap<String, String>();
        Map<String, IMonitorType> monitorTypes = new HashMap<String, IMonitorType>();
        monitorTypes.put("log", monitorType);
        when(databasePlatform.getName()).thenReturn("Postgres");
        when(engine.getParameterService()).thenReturn(parameterService);
        when(symmetricDialect.getPlatform()).thenReturn(databasePlatform);
        when(symmetricDialect.getSqlReplacementTokens()).thenReturn(testMap);
        when(symmetricDialect.getPlatform().getSqlTemplate()).thenReturn(sqlTemplate);
        when(engine.getExtensionService()).thenReturn(extensionService);
        when(extensionService.getExtensionPointMap(IMonitorType.class)).thenReturn(monitorTypes);
        when(engine.getNodeService()).thenReturn(nodeService);
        when(nodeService.findIdentity()).thenReturn(identity);
        when(engine.getCacheManager()).thenReturn(cacheManager);
        when(cacheManager.getActiveMonitorsForNode(identity.getNodeGroupId(), identity.getExternalId())).thenReturn(activeMonitors);
        when(monitor.getType()).thenReturn("log");
        when(monitor.getRunPeriod()).thenReturn(99999999);
        when(monitorType.check(monitor)).thenReturn(monitorEvent);
        when(monitorType.requiresClusterLock()).thenReturn(false);
        when(monitor.getRunCount()).thenReturn(1);
        when(engine.getClusterService()).thenReturn(clusterService);
        when(clusterService.lock("Monitor")).thenReturn(false);
        when(monitorEvent.getMonitorId()).thenReturn("log");
        MonitorService testMonitorService = new MonitorService(engine, symmetricDialect);
        testMonitorService.update();
        testMonitorService.update();
    }

    @Test
    void testRequiresClusterLockMonitorServiceUpdate() throws Exception {
        INodeService nodeService = mock(INodeService.class);
        ICacheManager cacheManager = mock(ICacheManager.class);
        Node identity = mock(Node.class);
        ISqlTemplate sqlTemplate = mock(ISqlTemplate.class);
        MonitorEvent monitorEvent = mock(MonitorEvent.class);
        Monitor monitor = mock(Monitor.class);
        MonitorTypeLog monitorType = mock(MonitorTypeLog.class);
        List<MonitorEvent> list = new ArrayList<MonitorEvent>();
        String sql = ArgumentMatchers.anyString();
        @SuppressWarnings("unchecked")
        ISqlRowMapper<MonitorEvent> mapper = (ISqlRowMapper<MonitorEvent>) ArgumentMatchers.any();
        when(sqlTemplate.query(sql, mapper, (Object[]) ArgumentMatchers.any())).thenAnswer(new Answer<List<MonitorEvent>>() {
            public List<MonitorEvent> answer(InvocationOnMock invocation) {
                list.add(monitorEvent);
                return list;
            }
        });
        List<Monitor> activeMonitors = new ArrayList<Monitor>();
        activeMonitors.add(monitor);
        Map<String, String> testMap = new HashMap<String, String>();
        Map<String, IMonitorType> monitorTypes = new HashMap<String, IMonitorType>();
        monitorTypes.put("log", monitorType);
        when(databasePlatform.getName()).thenReturn("Postgres");
        when(engine.getParameterService()).thenReturn(parameterService);
        when(symmetricDialect.getPlatform()).thenReturn(databasePlatform);
        when(symmetricDialect.getSqlReplacementTokens()).thenReturn(testMap);
        when(symmetricDialect.getPlatform().getSqlTemplate()).thenReturn(sqlTemplate);
        when(engine.getExtensionService()).thenReturn(extensionService);
        when(extensionService.getExtensionPointMap(IMonitorType.class)).thenReturn(monitorTypes);
        when(engine.getNodeService()).thenReturn(nodeService);
        when(nodeService.findIdentity()).thenReturn(identity);
        when(engine.getCacheManager()).thenReturn(cacheManager);
        when(cacheManager.getActiveMonitorsForNode(identity.getNodeGroupId(), identity.getExternalId())).thenReturn(activeMonitors);
        when(monitor.getType()).thenReturn("log");
        when(monitor.getRunPeriod()).thenReturn(99999999);
        when(monitorType.check(monitor)).thenReturn(monitorEvent);
        when(monitorType.requiresClusterLock()).thenReturn(true);
        when(monitor.getRunCount()).thenReturn(1);
        when(engine.getClusterService()).thenReturn(clusterService);
        when(clusterService.lock("Monitor")).thenReturn(false);
        when(monitorEvent.getMonitorId()).thenReturn("log");
        MonitorService testMonitorService = new MonitorService(engine, symmetricDialect);
        testMonitorService.update();
    }

    // This test case needs no Assertions as it is just testing if a null
    // identity throws an error for the if statement
    @Test
    void testNullIdentityMonitorServiceUpdate() throws Exception {
        ISqlTemplate sqlTemplate = mock(ISqlTemplate.class);
        MonitorEvent monitorEvent = mock(MonitorEvent.class);
        Monitor monitor = mock(Monitor.class);
        MonitorTypeLog monitorType = mock(MonitorTypeLog.class);
        List<MonitorEvent> list = new ArrayList<MonitorEvent>();
        String sql = ArgumentMatchers.anyString();
        @SuppressWarnings("unchecked")
        ISqlRowMapper<MonitorEvent> mapper = (ISqlRowMapper<MonitorEvent>) ArgumentMatchers.any();
        when(sqlTemplate.query(sql, mapper, (Object[]) ArgumentMatchers.any())).thenAnswer(new Answer<List<MonitorEvent>>() {
            public List<MonitorEvent> answer(InvocationOnMock invocation) {
                list.add(monitorEvent);
                return list;
            }
        });
        List<Monitor> activeMonitors = new ArrayList<Monitor>();
        activeMonitors.add(monitor);
        Map<String, String> testMap = new HashMap<String, String>();
        Map<String, IMonitorType> monitorTypes = new HashMap<String, IMonitorType>();
        monitorTypes.put("log", monitorType);
        when(databasePlatform.getName()).thenReturn("Postgres");
        when(engine.getParameterService()).thenReturn(parameterService);
        when(symmetricDialect.getPlatform()).thenReturn(databasePlatform);
        when(symmetricDialect.getSqlReplacementTokens()).thenReturn(testMap);
        when(symmetricDialect.getPlatform().getSqlTemplate()).thenReturn(sqlTemplate);
        when(engine.getExtensionService()).thenReturn(extensionService);
        when(extensionService.getExtensionPointMap(IMonitorType.class)).thenReturn(monitorTypes);
        when(engine.getNodeService()).thenReturn(nodeService);
        MonitorService testMonitorService = new MonitorService(engine, symmetricDialect);
        testMonitorService.update();
    }

    @Test
    void testNullMonitorTypeMonitorServiceUpdate() throws Exception {
        INodeService nodeService = mock(INodeService.class);
        ICacheManager cacheManager = mock(ICacheManager.class);
        Node identity = mock(Node.class);
        ISqlTemplate sqlTemplate = mock(ISqlTemplate.class);
        MonitorEvent monitorEvent = mock(MonitorEvent.class);
        Monitor monitor = mock(Monitor.class);
        MonitorTypeLog monitorType = mock(MonitorTypeLog.class);
        List<MonitorEvent> list = new ArrayList<MonitorEvent>();
        String sql = ArgumentMatchers.anyString();
        @SuppressWarnings("unchecked")
        ISqlRowMapper<MonitorEvent> mapper = (ISqlRowMapper<MonitorEvent>) ArgumentMatchers.any();
        when(sqlTemplate.query(sql, mapper, (Object[]) ArgumentMatchers.any())).thenAnswer(new Answer<List<MonitorEvent>>() {
            public List<MonitorEvent> answer(InvocationOnMock invocation) {
                list.add(monitorEvent);
                return list;
            }
        });
        List<Monitor> activeMonitors = new ArrayList<Monitor>();
        activeMonitors.add(monitor);
        Map<String, String> testMap = new HashMap<String, String>();
        Map<String, IMonitorType> monitorTypes = new HashMap<String, IMonitorType>();
        monitorTypes.put("log", monitorType);
        when(databasePlatform.getName()).thenReturn("Postgres");
        when(engine.getParameterService()).thenReturn(parameterService);
        when(symmetricDialect.getPlatform()).thenReturn(databasePlatform);
        when(symmetricDialect.getSqlReplacementTokens()).thenReturn(testMap);
        when(symmetricDialect.getPlatform().getSqlTemplate()).thenReturn(sqlTemplate);
        when(engine.getExtensionService()).thenReturn(extensionService);
        when(extensionService.getExtensionPointMap(IMonitorType.class)).thenReturn(monitorTypes);
        when(engine.getNodeService()).thenReturn(nodeService);
        when(nodeService.findIdentity()).thenReturn(identity);
        when(engine.getCacheManager()).thenReturn(cacheManager);
        when(cacheManager.getActiveMonitorsForNode(identity.getNodeGroupId(), identity.getExternalId())).thenReturn(activeMonitors);
        when(monitor.getType()).thenReturn(null);
        when(monitor.getRunPeriod()).thenReturn(99999999);
        when(monitorType.check(monitor)).thenReturn(monitorEvent);
        when(monitorType.requiresClusterLock()).thenReturn(false);
        when(monitor.getRunCount()).thenReturn(1);
        when(engine.getClusterService()).thenReturn(clusterService);
        when(clusterService.lock("Monitor")).thenReturn(false);
        when(monitorEvent.getMonitorId()).thenReturn("log");
        MonitorService testMonitorService = new MonitorService(engine, symmetricDialect);
        testMonitorService.update();
    }

    @Test
    void testClusterLockTrueMonitorServiceUpdate() throws Exception {
        INodeService nodeService = mock(INodeService.class);
        ICacheManager cacheManager = mock(ICacheManager.class);
        Node identity = mock(Node.class);
        ISqlTemplate sqlTemplate = mock(ISqlTemplate.class);
        MonitorEvent monitorEvent = mock(MonitorEvent.class);
        Monitor monitor = mock(Monitor.class);
        MonitorTypeLog monitorType = mock(MonitorTypeLog.class);
        List<MonitorEvent> list = new ArrayList<MonitorEvent>();
        String sql = ArgumentMatchers.anyString();
        @SuppressWarnings("unchecked")
        ISqlRowMapper<MonitorEvent> mapper = (ISqlRowMapper<MonitorEvent>) ArgumentMatchers.any();
        when(sqlTemplate.query(sql, mapper, (Object[]) ArgumentMatchers.any())).thenAnswer(new Answer<List<MonitorEvent>>() {
            public List<MonitorEvent> answer(InvocationOnMock invocation) {
                list.add(monitorEvent);
                return list;
            }
        });
        List<Monitor> activeMonitors = new ArrayList<Monitor>();
        activeMonitors.add(monitor);
        Map<String, String> testMap = new HashMap<String, String>();
        Map<String, IMonitorType> monitorTypes = new HashMap<String, IMonitorType>();
        monitorTypes.put("log", monitorType);
        when(databasePlatform.getName()).thenReturn("Postgres");
        when(engine.getParameterService()).thenReturn(parameterService);
        when(engine.getContextService()).thenReturn(contextService);
        when(symmetricDialect.getPlatform()).thenReturn(databasePlatform);
        when(symmetricDialect.getSqlReplacementTokens()).thenReturn(testMap);
        when(symmetricDialect.getPlatform().getSqlTemplate()).thenReturn(sqlTemplate);
        when(engine.getExtensionService()).thenReturn(extensionService);
        when(extensionService.getExtensionPointMap(IMonitorType.class)).thenReturn(monitorTypes);
        when(engine.getNodeService()).thenReturn(nodeService);
        when(nodeService.findIdentity()).thenReturn(identity);
        when(engine.getCacheManager()).thenReturn(cacheManager);
        when(cacheManager.getActiveMonitorsForNode(identity.getNodeGroupId(), identity.getExternalId())).thenReturn(activeMonitors);
        when(monitor.getType()).thenReturn("log");
        when(monitor.getRunPeriod()).thenReturn(99999999);
        when(monitorType.check(monitor)).thenReturn(monitorEvent);
        when(monitorType.requiresClusterLock()).thenReturn(false);
        when(monitor.getRunCount()).thenReturn(1);
        when(engine.getClusterService()).thenReturn(clusterService);
        when(clusterService.lock("Monitor")).thenReturn(true);
        when(monitorEvent.getMonitorId()).thenReturn("log");
        when(contextService.getString(ContextConstants.MONITOR_LAST_CHECK_TIMES)).thenReturn(null);
        MonitorService testMonitorService = new MonitorService(engine, symmetricDialect);
        testMonitorService.update();
    }
}
