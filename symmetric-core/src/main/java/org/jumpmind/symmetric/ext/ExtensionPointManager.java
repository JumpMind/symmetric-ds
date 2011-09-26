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
package org.jumpmind.symmetric.ext;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.jumpmind.symmetric.common.logging.ILog;
import org.jumpmind.symmetric.common.logging.LogFactory;
import org.jumpmind.symmetric.config.INodeIdGenerator;
import org.jumpmind.symmetric.config.IParameterFilter;
import org.jumpmind.symmetric.config.ITriggerCreationListener;
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.extract.IExtractorFilter;
import org.jumpmind.symmetric.io.IOfflineClientListener;
import org.jumpmind.symmetric.load.IBatchListener;
import org.jumpmind.symmetric.load.IColumnFilter;
import org.jumpmind.symmetric.load.IDataLoaderFilter;
import org.jumpmind.symmetric.load.IReloadListener;
import org.jumpmind.symmetric.load.ITableColumnFilter;
import org.jumpmind.symmetric.route.IBatchAlgorithm;
import org.jumpmind.symmetric.route.IDataRouter;
import org.jumpmind.symmetric.security.INodePasswordFilter;
import org.jumpmind.symmetric.service.IAcknowledgeService;
import org.jumpmind.symmetric.service.IDataExtractorService;
import org.jumpmind.symmetric.service.IDataLoaderService;
import org.jumpmind.symmetric.service.IDataService;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IOfflineDetectorService;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.service.IRegistrationService;
import org.jumpmind.symmetric.service.IRouterService;
import org.jumpmind.symmetric.service.ITriggerRouterService;
import org.jumpmind.symmetric.transform.IColumnTransform;
import org.jumpmind.symmetric.transform.ITransformService;
import org.jumpmind.symmetric.transport.IAcknowledgeEventListener;
import org.jumpmind.symmetric.transport.ISyncUrlExtension;
import org.jumpmind.symmetric.transport.ITransportManager;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.context.ApplicationContext;

/**
 * This manager registers {@link IExtensionPoint}s defined both by SymmetricDS
 * and others found in the {@link ApplicationContext}.
 * <P>
 * SymmetricDS reads in any Spring XML file found in the classpath of the
 * application that matches the following pattern:
 * /META-INF/services/symmetric-*-ext.xml
 */
public class ExtensionPointManager implements IExtensionPointManager, BeanFactoryAware {

    final ILog log = LogFactory.getLog(getClass());

    private IDataLoaderService dataLoaderService;

    private IDataService dataService;

    private IDataExtractorService dataExtractorService;

    private IParameterService parameterService;

    private INodeService nodeService;

    private IAcknowledgeService acknowledgeService;

    private List<IOfflineDetectorService> offlineDetectorServices;

    private IRegistrationService registrationService;

    private ITriggerRouterService triggerRouterService;

    private ITransportManager transportManager;

    private IRouterService routingService;
    
    private ITransformService transformService;
    
    private IDbDialect dbDialect;

    private BeanFactory beanFactory;

    private boolean initialized = false;
    
    private List<ExtensionPointMetaData> extensionPoints = new ArrayList<ExtensionPointMetaData>();

    public void register() throws BeansException {        
        ConfigurableListableBeanFactory cfgBeanFactory = (ConfigurableListableBeanFactory) beanFactory;
        if (!initialized) {
            Map<String, IExtensionPoint> extensions = new TreeMap<String, IExtensionPoint>();
            extensions.putAll(cfgBeanFactory.getBeansOfType(IExtensionPoint.class));
            if (cfgBeanFactory.getParentBeanFactory() != null
                    && cfgBeanFactory.getParentBeanFactory() instanceof ListableBeanFactory) {
                extensions.putAll(((ListableBeanFactory) cfgBeanFactory.getParentBeanFactory())
                        .getBeansOfType(IExtensionPoint.class));
            }
            for (String beanName : extensions.keySet()) {
                IExtensionPoint ext = extensions.get(beanName);
                boolean registered = false;
                if (ext.isAutoRegister()) {
                    boolean registerExtension = false;
                    if (ext instanceof INodeGroupExtensionPoint) {
                        String nodeGroupId = parameterService.getNodeGroupId();
                        INodeGroupExtensionPoint nodeExt = (INodeGroupExtensionPoint) ext;
                        String[] ids = nodeExt.getNodeGroupIdsToApplyTo();
                        if (ids != null) {
                            for (String targetNodeGroupId : ids) {
                                if (nodeGroupId.equals(targetNodeGroupId)) {
                                    registerExtension = true;
                                }
                            }
                        } else {
                            registerExtension = true;
                        }
                    } else {
                        registerExtension = true;
                    }

                    if (registerExtension) {
                        registered = registerExtension(beanName, ext);
                    }
                }
                
                if (!registered) {
                    extensionPoints.add(new ExtensionPointMetaData(ext, beanName, null, false));
                }
            }
            this.initialized = true;
        }

    }    
    
    public List<ExtensionPointMetaData> getExtensionPoints() {
        return new ArrayList<ExtensionPointMetaData>(extensionPoints);
    }

    protected boolean registerExtension(String beanName, IExtensionPoint ext) {
        boolean installed = false;
        if (ext instanceof IBuiltInExtensionPoint) {
            log.debug("ExtensionRegistering", beanName, ext.getClass().getSimpleName());
        } else {
            log.info("ExtensionRegistering", beanName, ext.getClass().getSimpleName());
        }

        if (ext instanceof ISyncUrlExtension) {
            installed = true;
            extensionPoints.add(new ExtensionPointMetaData(ext, beanName, ISyncUrlExtension.class, true));
            transportManager.addExtensionSyncUrlHandler(beanName, (ISyncUrlExtension) ext);
        }

        if (ext instanceof INodePasswordFilter) {
            installed = true;
            extensionPoints.add(new ExtensionPointMetaData(ext, beanName, INodePasswordFilter.class, true));
            nodeService.setNodePasswordFilter((INodePasswordFilter) ext);
            registrationService.setNodePasswordFilter((INodePasswordFilter) ext);
        }

        if (ext instanceof IAcknowledgeEventListener) {
            installed = true;
            extensionPoints.add(new ExtensionPointMetaData(ext, beanName, IAcknowledgeEventListener.class, true));
            acknowledgeService.addAcknowledgeEventListener((IAcknowledgeEventListener) ext);
        }

        if (ext instanceof ITriggerCreationListener) {
            installed = true;
            extensionPoints.add(new ExtensionPointMetaData(ext, beanName, ITriggerCreationListener.class, true));
            triggerRouterService.addTriggerCreationListeners((ITriggerCreationListener) ext);
        }

        if (ext instanceof IBatchListener) {
            installed = true;
            extensionPoints.add(new ExtensionPointMetaData(ext, beanName, IBatchListener.class, true));
            dataLoaderService.addBatchListener((IBatchListener) ext);
        }

        if (ext instanceof IHeartbeatListener) {
            installed = true;
            extensionPoints.add(new ExtensionPointMetaData(ext, beanName, IHeartbeatListener.class, true));
            dataService.addHeartbeatListener((IHeartbeatListener) ext);
        }

        if (ext instanceof IDataLoaderFilter) {
            installed = true;
            extensionPoints.add(new ExtensionPointMetaData(ext, beanName, IDataLoaderFilter.class, true));
            dataLoaderService.addDataLoaderFilter((IDataLoaderFilter) ext);
        }

        if (ext instanceof IColumnFilter) {
            if (ext instanceof ITableColumnFilter) {
                ITableColumnFilter tableColumnFilter = (ITableColumnFilter) ext;
                if (tableColumnFilter.getTables() != null) {
                    String[] tables = tableColumnFilter.getTables();
                    for (String table : tables) {
                        installed = true;
                        extensionPoints.add(new ExtensionPointMetaData(ext, beanName, ITableColumnFilter.class, true, table));
                        dataLoaderService.addColumnFilter(table, tableColumnFilter);
                    }
                }

            } else {
                throw new UnsupportedOperationException(
                        "IColumnFilter cannot be auto registered.  Please use "
                                + ITableColumnFilter.class.getName() + " instead.");
            }
        }

        if (ext instanceof IReloadListener) {
            installed = true;
            extensionPoints.add(new ExtensionPointMetaData(ext, beanName, IReloadListener.class, true));
            dataService.addReloadListener((IReloadListener) ext);
        }

        if (ext instanceof IParameterFilter) {
            installed = true;
            extensionPoints.add(new ExtensionPointMetaData(ext, beanName, IParameterFilter.class, true));
            parameterService.setParameterFilter((IParameterFilter) ext);
        }

        if (ext instanceof IExtractorFilter) {
            installed = true;
            extensionPoints.add(new ExtensionPointMetaData(ext, beanName, IExtractorFilter.class, true));
            dataExtractorService.addExtractorFilter((IExtractorFilter) ext);
        }

        if (ext instanceof INodeIdGenerator) {
            installed = true;
            extensionPoints.add(new ExtensionPointMetaData(ext, beanName, INodeIdGenerator.class, true));
            nodeService.setNodeIdGenerator((INodeIdGenerator) ext);
        }

        if (ext instanceof IDataRouter) {
            installed = true;
            extensionPoints.add(new ExtensionPointMetaData(ext, beanName, IDataRouter.class, true));
            routingService.addDataRouter(beanName, (IDataRouter) ext);
        }

        if (ext instanceof IBatchAlgorithm) {
            installed = true;
            extensionPoints.add(new ExtensionPointMetaData(ext, beanName, IBatchAlgorithm.class, true));
            routingService.addBatchAlgorithm(beanName, (IBatchAlgorithm) ext);
        }

        if (ext instanceof IOfflineClientListener) {
            for (IOfflineDetectorService service : offlineDetectorServices) {
                installed = true;
                extensionPoints.add(new ExtensionPointMetaData(ext, beanName, IOfflineClientListener.class, true));
                service.addOfflineListener((IOfflineClientListener) ext);
            }
        }

        if (ext instanceof IOfflineServerListener) {
            installed = true;
            extensionPoints.add(new ExtensionPointMetaData(ext, beanName, IOfflineServerListener.class, true));
            nodeService.addOfflineServerListener((IOfflineServerListener) ext);
        }        
        
        if (ext instanceof IExtraConfigTables) {
            triggerRouterService.addExtraConfigTables((IExtraConfigTables)ext);
        }
        
        if (ext instanceof IDatabaseUpgradeListener) {
            dbDialect.addDatabaseUpgradeListener((IDatabaseUpgradeListener)ext);
        }
        
        if (ext instanceof IColumnTransform) {
            IColumnTransform t = (IColumnTransform)ext;
            transformService.addColumnTransform(t.getName(), t);
        }

        return installed;
    }   

    public void setDataLoaderService(IDataLoaderService dataLoaderService) {
        this.dataLoaderService = dataLoaderService;
    }

    public void setDataService(IDataService dataService) {
        this.dataService = dataService;
    }

    public void setDataExtractorService(IDataExtractorService dataExtractorService) {
        this.dataExtractorService = dataExtractorService;
    }

    public void setParameterService(IParameterService parameterService) {
        this.parameterService = parameterService;
    }

    public void setNodeService(INodeService nodeService) {
        this.nodeService = nodeService;
    }

    public void setAcknowledgeService(IAcknowledgeService acknowledgeService) {
        this.acknowledgeService = acknowledgeService;
    }

    public void setRegistrationService(IRegistrationService registrationService) {
        this.registrationService = registrationService;
    }

    public void setTransportManager(ITransportManager transportManager) {
        this.transportManager = transportManager;
    }

    public void setRoutingService(IRouterService routingService) {
        this.routingService = routingService;
    }

    public void setTriggerRouterService(ITriggerRouterService triggerService) {
        this.triggerRouterService = triggerService;
    }

    public void setOfflineDetectorServices(List<IOfflineDetectorService> offlineDetectorServices) {
        this.offlineDetectorServices = offlineDetectorServices;
    }

    public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
        this.beanFactory = beanFactory;
    }
    
    public void setDbDialect(IDbDialect dbDialect) {
        this.dbDialect = dbDialect;
    }
 
    public void setTransformService(ITransformService transformService) {
        this.transformService = transformService;
    }
}