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

package org.jumpmind.symmetric.db;

import javax.sql.DataSource;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.FactoryBean;

/**
 * Factory that will create either a jndiDataSource or a basicDataSource based
 * on whether a JNDI name is provided.
 */
public class DataSourceFactoryBean implements FactoryBean, BeanFactoryAware {

    private String jndiName;

    private String beanName;

    private BeanFactory beanFactory;

    public Object getObject() throws Exception {
        if (jndiName == null || jndiName.trim().length() == 0) {
            return beanFactory.getBean(beanName);
        } else {
            return beanFactory.getBean("jndiDataSource");
        }
    }

    public Class<DataSource> getObjectType() {
        return DataSource.class;
    }

    public boolean isSingleton() {
        return true;
    }

    public void setBeanFactory(BeanFactory beanFactory) {
        this.beanFactory = beanFactory;
    }

    public void setJndiName(String jndiName) {
        this.jndiName = jndiName;
    }

    public void setBeanName(String beanName) {
        this.beanName = beanName;
    }

}
