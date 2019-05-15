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

import java.io.File;
import java.math.BigDecimal;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.sql.SqlException;
import org.jumpmind.properties.TypedProperties;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.service.IExtensionService;
import org.jumpmind.symmetric.util.SymmetricUtils;
import org.jumpmind.util.AppUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import bsh.EvalError;
import bsh.Interpreter;

abstract public class AbstractParameterService {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    protected IExtensionService extensionService;
    
    protected TypedProperties parameters;

    private long cacheTimeoutInMs = 0;

    private long lastTimeParameterWereCached;

    protected Properties systemProperties;

    protected boolean databaseHasBeenInitialized = false;

    protected String externalId=null;
    
    protected String engineName=null;

    protected String nodeGroupId=null;

    protected String syncUrl=null;

    protected String registrationUrl=null;

    public AbstractParameterService() {
        this.systemProperties = (Properties) System.getProperties().clone();
    }

    public BigDecimal getDecimal(String key, BigDecimal defaultVal) {
        String val = getString(key);
        if (val != null) {
            return new BigDecimal(val);
        }
        return defaultVal;
    }

    public BigDecimal getDecimal(String key) {
        return getDecimal(key, BigDecimal.ZERO);
    }

    public boolean is(String key) {
        return is(key, false);
    }

    public boolean is(String key, boolean defaultVal) {
        String val = getString(key);
        if (val != null) {
            val = val.trim();
            if (val.equals("1")) {
                return true;
            } else {
                return Boolean.parseBoolean(val);
            }
        } else {
            return defaultVal;
        }
    }

    public int getInt(String key) {
        return getInt(key, 0);
    }

    public int getInt(String key, int defaultVal) {
        String val = getString(key);
        if (StringUtils.isNotBlank(val)) {
            return Integer.parseInt(val.trim());
        }
        return defaultVal;
    }

    public long getLong(String key) {
        return getLong(key, 0);
    }

    public long getLong(String key, long defaultVal) {
        String val = getString(key);
        if (val != null) {
            return Long.parseLong(val);
        }
        return defaultVal;
    }

    public String getString(String key, String defaultVal) {
        String value = null;

        if (StringUtils.isBlank(value)) {
            value = getParameters().get(key, defaultVal);
        }

        return StringUtils.isBlank(value) ? defaultVal : value;
    }

    public String getString(String key) {
        return getString(key, null);
    }

    public String getTempDirectory() {
        String engineName = this.getEngineName();
        String tmpDirBase = getString("java.io.tmpdir", System.getProperty("java.io.tmpdir"));

        if (StringUtils.trimToNull(engineName) == null) {
            return tmpDirBase;
        } else {
            return tmpDirBase + File.separator + engineName;
        }
    }

    protected abstract TypedProperties rereadApplicationParameters();

    public synchronized void rereadParameters() {
        lastTimeParameterWereCached = 0;
        getParameters();
    }

    protected synchronized TypedProperties getParameters() {
        long timeoutTime = System.currentTimeMillis() - cacheTimeoutInMs;
        // see if the parameters have timed out
        if (parameters == null || (cacheTimeoutInMs > 0 && lastTimeParameterWereCached < timeoutTime)) {
            try {
                parameters = rereadApplicationParameters();
                SymmetricUtils.replaceSystemAndEnvironmentVariables(parameters);
                lastTimeParameterWereCached = System.currentTimeMillis();
                cacheTimeoutInMs = getInt(ParameterConstants.PARAMETER_REFRESH_PERIOD_IN_MS);
            } catch (SqlException ex) {
                if (parameters != null) {
                    log.warn("Could not read database parameters.  We will try again later", ex);
                } else {
                    log.error("Could not read database parameters and they have not yet been initialized");
                    throw ex;
                }
                throw ex;
            }
        }
        return parameters;
    }

    public TypedProperties getAllParameters() {
        return getParameters();
    }

    public Date getLastTimeParameterWereCached() {
        return new Date(lastTimeParameterWereCached);
    }

   
    public synchronized String getExternalId() {
    	if (externalId==null) {
    		String value = getString(ParameterConstants.EXTERNAL_ID);
    		value = substituteScripts(value);
    		externalId = value;
    		if (log.isDebugEnabled()) {
        		log.debug("External Id eval results in: {}",externalId);
        	}
    	}
    	return externalId;
    }

    public synchronized  String getSyncUrl() {
    	if (syncUrl==null) {
    		String value = getString(ParameterConstants.SYNC_URL);
    		value = substituteScripts(value);
    		if (value != null) {
    			value = value.trim();
            }
    		syncUrl = value;
    		if (log.isDebugEnabled()) {
        		log.debug("Sync URL eval results in: {}",syncUrl);
        	}
    	}
    	return syncUrl;
    }

    public synchronized String getNodeGroupId() {
    	if (nodeGroupId==null) {
    		String value = getString(ParameterConstants.NODE_GROUP_ID);
    		value = substituteScripts(value);
    		nodeGroupId = value;
    		if (log.isDebugEnabled()) {
        		log.debug("Node Group Id eval results in: {}",nodeGroupId);
        	}
    	}
    	return nodeGroupId;
    }

    public synchronized  String getRegistrationUrl() {
    	if (registrationUrl==null) {
    		String value = getString(ParameterConstants.REGISTRATION_URL);
    		value = substituteScripts(value);
    		if (value != null) {
    			value = value.trim();
            }
    		registrationUrl = value;
    		if (log.isDebugEnabled()) {
        		log.debug("Registration URL eval results in: {}",registrationUrl);
        	}
    	}
    	return registrationUrl;
    }

    public synchronized  String getEngineName() {
    	if (engineName==null) {
    		String value = getString(ParameterConstants.ENGINE_NAME,"SymmetricDS");
    		value = substituteScripts(value);
    		engineName = value;
        	if (log.isDebugEnabled()) {
        		log.debug("Engine Name eval results in: {}",engineName);
        	}        	
        }
        return engineName;
    }

    public Map<String, String> getReplacementValues() {
        Map<String, String> replacementValues = new HashMap<String, String>(2);
        replacementValues.put("nodeGroupId", getNodeGroupId());
        replacementValues.put("externalId", getExternalId());
        replacementValues.put("engineName", getEngineName());
        replacementValues.put("syncUrl", getSyncUrl());
        replacementValues.put("registrationUrl", getRegistrationUrl());
        return replacementValues;
    }
    
    public synchronized void setDatabaseHasBeenInitialized(boolean databaseHasBeenInitialized) {
        if (this.databaseHasBeenInitialized != databaseHasBeenInitialized) {
            this.databaseHasBeenInitialized = databaseHasBeenInitialized;
            this.parameters = null;
        }
    }

    abstract public TypedProperties getDatabaseParameters(String externalId, String nodeGroupId);

    protected synchronized TypedProperties rereadDatabaseParameters(Properties p) {
        if (databaseHasBeenInitialized) {
            TypedProperties properties = getDatabaseParameters(ParameterConstants.ALL,
                    ParameterConstants.ALL);
            properties.putAll(getDatabaseParameters(ParameterConstants.ALL,
                    p.getProperty(ParameterConstants.NODE_GROUP_ID)));
            properties.putAll(getDatabaseParameters(
                    p.getProperty(ParameterConstants.EXTERNAL_ID),
                    p.getProperty(ParameterConstants.NODE_GROUP_ID)));
            return properties;
        } else {
            return new TypedProperties();
        }
    }
    	

    public void setExtensionService(IExtensionService extensionService) {
        this.extensionService = extensionService;
    }

    
    protected  String substituteScripts(String value) {
    	if (log.isDebugEnabled()) {
			log.debug("substituteScripts starting value is: {}",value);
		}
		int startTick = StringUtils.indexOf(value, '`');
		if (startTick!=-1) {
    		int endTick = StringUtils.lastIndexOf(value, '`');
    		if (endTick!=-1 && startTick!=endTick) {
    			// there's a bean shell script present in this case
    			String script = StringUtils.substring(value, startTick+1,endTick);
    			if (log.isDebugEnabled()) {
        			log.debug("Script found.  Script is is: {}",script);
        		}
    			
                Interpreter interpreter = new Interpreter();
                try {
					interpreter.set("hostName",  AppUtils.getHostName());
	                interpreter.set("log", log);
	                interpreter.set("nodeGroupId", nodeGroupId);
	                interpreter.set("syncUrl", syncUrl);
	                interpreter.set("registrationUrl", registrationUrl);
	                interpreter.set("externalId", externalId);
	                interpreter.set("engineName", engineName);
	                
	                Object scriptResult = interpreter.eval(script);
	          
	                if (scriptResult==null) {
	                	scriptResult ="";
	                }
	                
	                if (log.isDebugEnabled()) {
	    	    		log.debug("Script output is: {}",scriptResult);
	    	    	}
	               	value = StringUtils.substring(value, 0,startTick) + scriptResult.toString() +
	        					StringUtils.substring(value, endTick+1);
				} catch (EvalError e) {
					throw new RuntimeException(e.getMessage(),e);
				}
        		
    			if (log.isDebugEnabled()) {
        			log.debug("substituteScripts return value is {}",value);
        		}
       		}
		}
		return value;
    }

}