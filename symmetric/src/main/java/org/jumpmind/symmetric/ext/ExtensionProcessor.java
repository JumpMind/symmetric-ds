package org.jumpmind.symmetric.ext;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.config.IParameterFilter;
import org.jumpmind.symmetric.extract.IExtractorFilter;
import org.jumpmind.symmetric.load.IBatchListener;
import org.jumpmind.symmetric.load.IColumnFilter;
import org.jumpmind.symmetric.load.IDataLoaderFilter;
import org.jumpmind.symmetric.load.INodeGroupDataLoaderFilter;
import org.jumpmind.symmetric.load.IReloadListener;
import org.jumpmind.symmetric.load.ITableColumnFilter;
import org.jumpmind.symmetric.service.IDataExtractorService;
import org.jumpmind.symmetric.service.IDataLoaderService;
import org.jumpmind.symmetric.service.IDataService;
import org.jumpmind.symmetric.service.IParameterService;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;

public class ExtensionProcessor implements BeanFactoryPostProcessor {

    static final Log logger = LogFactory.getLog(ExtensionProcessor.class);

    IDataLoaderService dataLoaderService;

    IDataService dataService;

    IDataExtractorService dataExtractorService;

    IParameterService parameterService;

    @SuppressWarnings("unchecked")
    public void postProcessBeanFactory(ConfigurableListableBeanFactory beanFactory) throws BeansException {
        Map<String, IExtensionPoint> plugins = beanFactory.getBeansOfType(IExtensionPoint.class);
        for (IExtensionPoint plugin : plugins.values()) {
            if (plugin.isAutoRegister()) {
                if (plugin instanceof IBatchListener) {
                    dataLoaderService.addBatchListener((IBatchListener) plugin);
                }

                if (plugin instanceof INodeGroupDataLoaderFilter) {
                    String nodeGroupId = parameterService.getNodeGroupId();
                    INodeGroupDataLoaderFilter filter = (INodeGroupDataLoaderFilter) plugin;
                    String[] ids = filter.getNodeGroupIdsToApplyTo();
                    if (ids != null) {
                        for (String targetNodeGroupId : ids) {
                            if (nodeGroupId.equals(targetNodeGroupId)) {
                                dataLoaderService.addDataLoaderFilter(filter);
                            }
                        }
                    }
                } else if (plugin instanceof IDataLoaderFilter) {
                    dataLoaderService.addDataLoaderFilter((IDataLoaderFilter) plugin);
                }

                if (plugin instanceof IColumnFilter) {
                    if (plugin instanceof ITableColumnFilter) {
                        ITableColumnFilter tableColumnFilter = (ITableColumnFilter) plugin;
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

                if (plugin instanceof IReloadListener) {
                    dataService.addReloadListener((IReloadListener) plugin);
                }

                if (plugin instanceof IParameterFilter) {
                    parameterService.setParameterFilter((IParameterFilter) plugin);
                }

                if (plugin instanceof IExtractorFilter) {
                    dataExtractorService.addExtractorFilter((IExtractorFilter) plugin);
                }
            }
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

}
