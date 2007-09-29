package org.jumpmind.symmetric.db;

import javax.sql.DataSource;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.FactoryBean;

/**
 * Factory that will create either a jndiDataSource or a basicDataSource based on whether a JNDI name
 * is provided.
 */
public class DataSourceFactoryBean implements FactoryBean, BeanFactoryAware {

    private String jndiName;

    private BeanFactory beanFactory;

    public Object getObject() throws Exception {
        if (jndiName == null || jndiName.trim().length() == 0) {
            return beanFactory.getBean("basicDataSource");
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

}
