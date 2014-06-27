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
package org.jumpmind.symmetric.ext;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.jumpmind.extension.IBuiltInExtensionPoint;
import org.jumpmind.extension.IExtensionPoint;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.config.INodeIdCreator;
import org.jumpmind.symmetric.config.INodeIdGenerator;
import org.jumpmind.symmetric.config.IParameterFilter;
import org.jumpmind.symmetric.config.ITriggerCreationListener;
import org.jumpmind.symmetric.config.NodeIdCreatorAdaptor;
import org.jumpmind.symmetric.io.IOfflineClientListener;
import org.jumpmind.symmetric.io.data.transform.IColumnTransform;
import org.jumpmind.symmetric.io.data.writer.IDatabaseWriterErrorHandler;
import org.jumpmind.symmetric.io.data.writer.IDatabaseWriterFilter;
import org.jumpmind.symmetric.io.data.writer.TransformWriter;
import org.jumpmind.symmetric.load.IDataLoaderFactory;
import org.jumpmind.symmetric.load.IReloadListener;
import org.jumpmind.symmetric.route.IBatchAlgorithm;
import org.jumpmind.symmetric.route.IDataRouter;
import org.jumpmind.symmetric.security.INodePasswordFilter;
import org.jumpmind.symmetric.transport.IAcknowledgeEventListener;
import org.jumpmind.symmetric.transport.ISyncUrlExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.context.ApplicationContext;

/**
 * This manager registers {@link IExtensionPoint}s defined both by SymmetricDS
 * and others found in the {@link ApplicationContext}.
 * <P>
 * SymmetricDS reads in any Spring XML file found in the classpath of the
 * application that matches the following pattern:
 * /META-INF/services/symmetric-*-ext.xml
 */
public class ExtensionPointManager implements IExtensionPointManager {

    private final static Logger log = LoggerFactory.getLogger(ExtensionPointManager.class);

    private boolean initialized = false;

    private ISymmetricEngine engine;

    private Map<String, IExtensionPoint> extensions;

    private List<ExtensionPointMetaData> extensionPoints = new ArrayList<ExtensionPointMetaData>();

    private ApplicationContext springContext;

    public ExtensionPointManager(ISymmetricEngine engine, ApplicationContext springContext) {
        this.engine = engine;
        this.springContext = springContext;
    }

    public void register() {

        if (!initialized) {
            extensions = new TreeMap<String, IExtensionPoint>();
            extensions.putAll(springContext.getBeansOfType(IExtensionPoint.class));
            if (springContext.getParentBeanFactory() != null
                    && springContext.getParentBeanFactory() instanceof ListableBeanFactory) {
                extensions.putAll(((ListableBeanFactory) springContext.getParentBeanFactory())
                        .getBeansOfType(IExtensionPoint.class));
            }
            
            log.info("Found {} extension points that will be registered", extensions.size());
            
            for (String beanName : extensions.keySet()) {
                IExtensionPoint ext = extensions.get(beanName);
                if (ext instanceof ISymmetricEngineAware) {
                    ((ISymmetricEngineAware) ext).setSymmetricEngine(engine);
                }
                boolean registered = false;
                boolean registerExtension = false;
                if (ext instanceof INodeGroupExtensionPoint) {
                    String nodeGroupId = engine.getParameterService().getNodeGroupId();
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
            log.debug("Registering an extension point named {} of type '{}' with SymmetricDS",
                    beanName, ext.getClass().getSimpleName());
        } else {
            log.info("Registering an extension point named {} of type '{}' with SymmetricDS",
                    beanName, ext.getClass().getSimpleName());
        }

        if (ext instanceof ISyncUrlExtension) {
            installed = true;
            extensionPoints.add(new ExtensionPointMetaData(ext, beanName, ISyncUrlExtension.class,
                    true));
            engine.getTransportManager().addExtensionSyncUrlHandler(beanName,
                    (ISyncUrlExtension) ext);
        }

        if (ext instanceof INodePasswordFilter) {
            installed = true;
            extensionPoints.add(new ExtensionPointMetaData(ext, beanName,
                    INodePasswordFilter.class, true));
            engine.getNodeService().setNodePasswordFilter((INodePasswordFilter) ext);
            engine.getRegistrationService().setNodePasswordFilter((INodePasswordFilter) ext);
        }

        if (ext instanceof IDataLoaderFactory) {
            installed = true;
            extensionPoints.add(new ExtensionPointMetaData(ext, beanName, IDataLoaderFactory.class,
                    true));
            engine.getDataLoaderService().addDataLoaderFactory((IDataLoaderFactory) ext);
        }

        if (ext instanceof IAcknowledgeEventListener) {
            installed = true;
            extensionPoints.add(new ExtensionPointMetaData(ext, beanName,
                    IAcknowledgeEventListener.class, true));
            engine.getAcknowledgeService().addAcknowledgeEventListener(
                    (IAcknowledgeEventListener) ext);
        }

        if (ext instanceof ITriggerCreationListener) {
            installed = true;
            extensionPoints.add(new ExtensionPointMetaData(ext, beanName,
                    ITriggerCreationListener.class, true));
            engine.getTriggerRouterService().addTriggerCreationListeners(
                    (ITriggerCreationListener) ext);
        }

        if (ext instanceof IDatabaseWriterFilter) {
            installed = true;
            extensionPoints.add(new ExtensionPointMetaData(ext, beanName,
                    IDatabaseWriterFilter.class, true));
            engine.getDataLoaderService().addDatabaseWriterFilter((IDatabaseWriterFilter) ext);
        }
        
        if (ext instanceof IDatabaseWriterErrorHandler) {
            installed = true;
            extensionPoints.add(new ExtensionPointMetaData(ext, beanName,
                    IDatabaseWriterErrorHandler.class, true));
            engine.getDataLoaderService().addDatabaseWriterErrorHandler((IDatabaseWriterErrorHandler) ext);
        }        

        if (ext instanceof IHeartbeatListener) {
            installed = true;
            extensionPoints.add(new ExtensionPointMetaData(ext, beanName, IHeartbeatListener.class,
                    true));
            engine.getDataService().addHeartbeatListener((IHeartbeatListener) ext);
        }

        if (ext instanceof IReloadListener) {
            installed = true;
            extensionPoints.add(new ExtensionPointMetaData(ext, beanName, IReloadListener.class,
                    true));
            engine.getDataService().addReloadListener((IReloadListener) ext);
        }

        if (ext instanceof IParameterFilter) {
            installed = true;
            extensionPoints.add(new ExtensionPointMetaData(ext, beanName, IParameterFilter.class,
                    true));
            engine.getParameterService().setParameterFilter((IParameterFilter) ext);
        }

        if (ext instanceof INodeIdGenerator) {
            installed = true;
            extensionPoints.add(new ExtensionPointMetaData(ext, beanName, INodeIdGenerator.class,
                    true));
            engine.getNodeService().setNodeIdCreator(
                    new NodeIdCreatorAdaptor((INodeIdGenerator) ext, engine.getNodeService()));
        }
        
        if (ext instanceof INodeIdCreator) {
            installed = true;
            extensionPoints.add(new ExtensionPointMetaData(ext, beanName, INodeIdCreator.class,
                    true));
            engine.getNodeService().setNodeIdCreator((INodeIdCreator) ext);
        }        

        if (ext instanceof IDataRouter) {
            installed = true;
            extensionPoints.add(new ExtensionPointMetaData(ext, beanName, IDataRouter.class, true));
            engine.getRouterService().addDataRouter(beanName, (IDataRouter) ext);
        }

        if (ext instanceof IBatchAlgorithm) {
            installed = true;
            extensionPoints.add(new ExtensionPointMetaData(ext, beanName, IBatchAlgorithm.class,
                    true));
            engine.getRouterService().addBatchAlgorithm(beanName, (IBatchAlgorithm) ext);
        }

        if (ext instanceof IOfflineClientListener) {
            installed = true;
            extensionPoints.add(new ExtensionPointMetaData(ext, beanName,
                    IOfflineClientListener.class, true));
            engine.getPushService().addOfflineListener((IOfflineClientListener) ext);
            engine.getPullService().addOfflineListener((IOfflineClientListener) ext);
        }

        if (ext instanceof IOfflineServerListener) {
            installed = true;
            extensionPoints.add(new ExtensionPointMetaData(ext, beanName,
                    IOfflineServerListener.class, true));
            engine.getNodeService().addOfflineServerListener((IOfflineServerListener) ext);
        }

        if (ext instanceof IDatabaseUpgradeListener) {
            engine.getSymmetricDialect().addDatabaseUpgradeListener((IDatabaseUpgradeListener) ext);
        }

        if (ext instanceof IColumnTransform) {
            IColumnTransform<?> t = (IColumnTransform<?>) ext;
            TransformWriter.addColumnTransform(t);
        }

        return installed;
    }

    @SuppressWarnings("unchecked")
    public <T extends IExtensionPoint> T getExtensionPoint(String name) {
        if (extensions != null) {
            return (T) extensions.get(name);
        } else {
            return null;
        }
    }

}
