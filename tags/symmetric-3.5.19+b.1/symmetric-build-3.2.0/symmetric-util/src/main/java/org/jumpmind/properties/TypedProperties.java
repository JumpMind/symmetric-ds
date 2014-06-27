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
package org.jumpmind.properties;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.jumpmind.exception.IoException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TypedProperties extends Properties {

    private static final long serialVersionUID = 1L;

    protected static Logger log = LoggerFactory.getLogger(TypedProperties.class);

    public TypedProperties() {
    }
    
    public TypedProperties(File file) {
        FileInputStream fis = null;
        try {
            fis = new FileInputStream(file);
            load(fis);
        } catch (IOException ex) {
            throw new IoException(ex);
        } finally {
            if (fis != null) {
                try {
                    fis.close();
                } catch (IOException e) {
                }
            }
        }
    }

    public TypedProperties(Properties properties) {
        this();
        putAll(properties);
    }

    public void putAll(Properties properties) {
        for (Object key : properties.keySet()) {
            put((String) key, properties.getProperty((String) key));
        }
    }
    
    public long getLong(String key) {
        return getLong(key, -1);
    }

    public long getLong(String key, long defaultValue) {
        long returnValue = defaultValue;
        String value = getProperty(key);
        if (value != null) {
            try {
                returnValue = Long.parseLong(value);
            } catch (NumberFormatException ex) {
            }
        }
        return returnValue;
    }
    
    public int getInt(String key) {
        return getInt(key, 0);
    }

    public int getInt(String key, int defaultValue) {
        int returnValue = defaultValue;
        String value = getProperty(key);
        if (value != null) {
            try {
                returnValue = Integer.parseInt(value);
            } catch (NumberFormatException ex) {
            }
        }
        return returnValue;
    }
    
    public boolean is(String key) {
        return is(key, false);
    }

    public boolean is(String key, boolean defaultValue) {
        boolean returnValue = defaultValue;
        String value = getProperty(key);
        if (value != null) {
            returnValue = Boolean.parseBoolean(value);
        }
        return returnValue;
    }
    
    public String get(String key) {
        return get(key, null);
    }

    public String get(String key, String defaultValue) {
        String returnValue = defaultValue;
        String value = getProperty(key);
        if (value != null) {
            returnValue = value;
        }
        return returnValue;
    }
    
    public void setProperty(String key, int value) {
        setProperty(key, Integer.toString(value));
    }
    
    public void setProperty(String key, long value) {
        setProperty(key, Long.toString(value));
    }
    
    public String[] getArray(String key, String[] defaultValue) {
        String value = getProperty(key);
        String[] retValue = defaultValue;
        if (value != null) {
            retValue = value.split(",");
        }
        return retValue;
    }

    @SuppressWarnings("unchecked")
    public <T> List<T> instantiate(String key) {
        String[] clazzes = getArray(key, new String[0]);
        List<T> objects = new ArrayList<T>(clazzes.length);
        try {
            for (String clazz : clazzes) {
                Class<?> c = Class.forName(clazz);
                if (c != null) {
                    objects.add((T) c.newInstance());
                }
            }
            return objects;
        } catch (Exception ex) {
            log.warn(ex.getMessage(), ex);
            return objects;
        }
    }

}
