package org.jumpmind.symmetric.service.impl;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.jumpmind.properties.TypedProperties;
import org.jumpmind.symmetric.model.DatabaseParameter;
import org.jumpmind.symmetric.service.IParameterService;

public class MockParameterService extends AbstractParameterService implements IParameterService {

    private Properties properties = new Properties();
    
    public MockParameterService() {

    }
    
    public MockParameterService(Properties properties) {
        this.properties = properties;
    }
    
    public boolean isRegistrationServer() {
        return false;
    }
    
    public void saveParameter(String key, Object paramValue) {
    }

    public void saveParameter(String externalId, String nodeGroupId, String key, Object paramValue) {
    }

    public void saveParameters(String externalId, String nodeGroupId, Map<String, Object> parameters) {
    }

    public void deleteParameter(String externalId, String nodeGroupId, String key) {
    }

    public List<DatabaseParameter> getDatabaseParametersFor(String paramKey) {
        return null;
    }

    public TypedProperties getDatabaseParametersByNodeGroupId(String nodeGroupId) {
        return null;
    }

    public String getTablePrefix() {
        return "sym";
    }

    @Override
    protected TypedProperties rereadApplicationParameters() {
        return new TypedProperties(properties);
    }

    @Override
    protected TypedProperties rereadDatabaseParameters(String externalId, String nodeGroupId) {
        return new TypedProperties(properties);
    }


}
