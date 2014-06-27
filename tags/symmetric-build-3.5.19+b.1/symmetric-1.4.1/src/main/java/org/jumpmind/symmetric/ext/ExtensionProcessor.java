package org.jumpmind.symmetric.ext;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.config.IParameterFilter;
import org.jumpmind.symmetric.extract.IExtractorFilter;
import org.jumpmind.symmetric.load.IBatchListener;
import org.jumpmind.symmetric.load.IColumnFilter;
import org.jumpmind.symmetric.load.IDataLoaderFilter;
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
        Map<String, IExtensionPoint> extensions = beanFactory.getBeansOfType(IExtensionPoint.class);
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
