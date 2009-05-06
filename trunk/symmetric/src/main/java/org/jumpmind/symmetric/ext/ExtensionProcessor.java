/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Chris Henson <chenson42@users.sourceforge.net>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see
 * <http://www.gnu.org/licenses/>.
 */
package org.jumpmind.symmetric.ext;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.config.ITriggerCreationListener;
import org.jumpmind.symmetric.config.INodeIdGenerator;
import org.jumpmind.symmetric.config.IParameterFilter;
import org.jumpmind.symmetric.extract.IExtractorFilter;
import org.jumpmind.symmetric.load.IBatchListener;
import org.jumpmind.symmetric.load.IColumnFilter;
import org.jumpmind.symmetric.load.IDataLoaderFilter;
import org.jumpmind.symmetric.load.IReloadListener;
import org.jumpmind.symmetric.load.ITableColumnFilter;
import org.jumpmind.symmetric.service.IAcknowledgeService;
import org.jumpmind.symmetric.service.IBootstrapService;
import org.jumpmind.symmetric.service.IDataExtractorService;
import org.jumpmind.symmetric.service.IDataLoaderService;
import org.jumpmind.symmetric.service.IDataService;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.transport.IAcknowledgeEventListener;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;

public class ExtensionProcessor implements BeanFactoryPostProcessor {

    static final Log logger = LogFactory.getLog(ExtensionProcessor.class);

    IDataLoaderService dataLoaderService;

    IDataService dataService;

    IDataExtractorService dataExtractorService;

    IParameterService parameterService;

    INodeService nodeService;

    IBootstrapService bootstrapService;
    
    IAcknowledgeService acknowledgeService;

    @SuppressWarnings("unchecked")
    public void postProcessBeanFactory(ConfigurableListableBeanFactory beanFactory) throws BeansException {
        Map<String, IExtensionPoint> extensions = new HashMap<String, IExtensionPoint>();
        extensions.putAll(beanFactory.getBeansOfType(IExtensionPoint.class));
        if (beanFactory.getParentBeanFactory() != null
                && beanFactory.getParentBeanFactory() instanceof ListableBeanFactory) {
            extensions.putAll(((ListableBeanFactory) beanFactory.getParentBeanFactory())
                    .getBeansOfType(IExtensionPoint.class));
        }
        for (IExtensionPoint ext : extensions.values()) {
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
                    }
                } else {
                    registerExtension = true;
                }

                if (registerExtension) {
                    registerExtension(ext);
                }
            }
        }

    }

    private void registerExtension(IExtensionPoint ext) {

        if (ext instanceof IAcknowledgeEventListener) {
        	acknowledgeService.addAcknowledgeEventListener((IAcknowledgeEventListener)ext);
        }
    	
        if (ext instanceof ITriggerCreationListener) {
            bootstrapService.addTriggerCreationListeners((ITriggerCreationListener) ext);
        }

        if (ext instanceof IBatchListener) {
            dataLoaderService.addBatchListener((IBatchListener) ext);
        }

        if (ext instanceof IDataLoaderFilter) {
            dataLoaderService.addDataLoaderFilter((IDataLoaderFilter) ext);
        }

        if (ext instanceof IColumnFilter) {
            if (ext instanceof ITableColumnFilter) {
                ITableColumnFilter tableColumnFilter = (ITableColumnFilter) ext;
                if (tableColumnFilter.getTables() != null) {
                    String[] tables = tableColumnFilter.getTables();
                    for (String table : tables) {
                        dataLoaderService.addColumnFilter(table, tableColumnFilter);
                    }
                }

            } else {
                throw new UnsupportedOperationException("IColumnFilter cannot be auto registered.  Please use "
                        + ITableColumnFilter.class.getName() + " instead.");
            }
        }

        if (ext instanceof IReloadListener) {
            dataService.addReloadListener((IReloadListener) ext);
        }

        if (ext instanceof IParameterFilter) {
            parameterService.setParameterFilter((IParameterFilter) ext);
        }

        if (ext instanceof IExtractorFilter) {
            dataExtractorService.addExtractorFilter((IExtractorFilter) ext);
        }

        if (ext instanceof INodeIdGenerator) {
            nodeService.setNodeIdGenerator((INodeIdGenerator) ext);
        }
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

    public void setBootstrapService(IBootstrapService bootstrapService) {
        this.bootstrapService = bootstrapService;
    }

	public void setAcknowledgeService(IAcknowledgeService acknowledgeService) {
		this.acknowledgeService = acknowledgeService;
	}

}
