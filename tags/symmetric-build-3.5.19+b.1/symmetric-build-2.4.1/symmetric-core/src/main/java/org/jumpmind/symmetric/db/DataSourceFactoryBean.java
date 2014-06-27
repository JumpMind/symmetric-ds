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


package org.jumpmind.symmetric.db;

import javax.sql.DataSource;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.lang.StringUtils;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.logging.ILog;
import org.jumpmind.symmetric.common.logging.LogFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

/**
 * Factory that will create either a jndiDataSource or a basicDataSource based
 * on whether a JNDI name is provided.
 */
public class DataSourceFactoryBean implements FactoryBean<DataSource>, ApplicationContextAware {

    static final ILog log = LogFactory.getLog(DataSourceFactoryBean.class);
    
    private String jndiName;

    private String beanName;

    private ApplicationContext applicationContext;
    
    private String connectionProperties;

    public DataSource getObject() throws Exception {
        DataSource dataSource = null;
        if (jndiName == null || jndiName.trim().length() == 0) {
            if (beanName.startsWith(Constants.PARENT_PROPERTY_PREFIX)
                    && applicationContext.getParent() != null) {
                dataSource = (DataSource) applicationContext.getParent().getBean(
                        beanName.substring(Constants.PARENT_PROPERTY_PREFIX.length()));
            } else {
                dataSource = (DataSource) applicationContext.getBean(beanName);
            }
        } else {
            dataSource = (DataSource) applicationContext.getBean("jndiDataSource");
        }
        
        applyConnectionProperties(dataSource);
        return dataSource;
    }
    
    protected void applyConnectionProperties(DataSource dataSource) {
        if (StringUtils.isNotBlank(connectionProperties) && dataSource instanceof BasicDataSource) {
            BasicDataSource bds = (BasicDataSource)dataSource;
            String[] properties = connectionProperties.split(";");
            for (String property : properties) {
                String[] keyValue = property.split("=");
                if (keyValue != null && keyValue.length > 1) {
                    log.info("DatabaseSettingConnectionProperty", keyValue[0], keyValue[1]);
                    bds.addConnectionProperty(keyValue[0], keyValue[1]);
                }
            }
        }
    }

    public Class<DataSource> getObjectType() {
        return DataSource.class;
    }

    public boolean isSingleton() {
        return true;
    }

    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    public void setJndiName(String jndiName) {
        this.jndiName = jndiName;
    }

    public void setBeanName(String beanName) {
        this.beanName = beanName;
    }
    
    public void setConnectionProperties(String connectionProperties) {
        this.connectionProperties = connectionProperties;
    }

}