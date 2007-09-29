package org.jumpmind.symmetric.service;

import java.math.BigDecimal;
import java.util.Map;

import org.jumpmind.symmetric.model.GlobalParameter;

/**
 * Get and set application wide configuration information.
 * @author chenson
 */
public interface IParameterService {

    public String getString(String configurationId, GlobalParameter key);

    public int getInt(String configurationId, GlobalParameter key);

    public BigDecimal getDecimal(String configurationId, GlobalParameter key);

    public long getLong(String configurationId, GlobalParameter key);

    public void saveParameter(String configurationId, String key, Object param);

    public void saveParameters(String configurationId,
            Map<String, Object> parameters);

    public void populateDefautGlobalParametersIfNeeded();
}
