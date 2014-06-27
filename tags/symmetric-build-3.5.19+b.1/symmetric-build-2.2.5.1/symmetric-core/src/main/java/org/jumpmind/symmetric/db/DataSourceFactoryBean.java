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

import org.jumpmind.symmetric.common.Constants;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

/**
 * Factory that will create either a jndiDataSource or a basicDataSource based
 * on whether a JNDI name is provided.
 *
 * 
 */
public class DataSourceFactoryBean implements FactoryBean<DataSource>, ApplicationContextAware {

    private String jndiName;

    private String beanName;

    private ApplicationContext applicationContext;

    public DataSource getObject() throws Exception {
        if (jndiName == null || jndiName.trim().length() == 0) {
            if (beanName.startsWith(Constants.PARENT_PROPERTY_PREFIX) && applicationContext.getParent() != null) {
                return (DataSource) applicationContext.getParent().getBean(
                        beanName.substring(Constants.PARENT_PROPERTY_PREFIX.length()));
            }
            return (DataSource) applicationContext.getBean(beanName);
        } else {
            return (DataSource) applicationContext.getBean("jndiDataSource");
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

}