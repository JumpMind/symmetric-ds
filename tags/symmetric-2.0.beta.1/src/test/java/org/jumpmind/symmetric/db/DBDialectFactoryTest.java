package org.jumpmind.symmetric.db;

import javax.sql.DataSource;

import org.jumpmind.symmetric.db.db2.Db2DbDialect;
import org.jumpmind.symmetric.db.db2.Db2zSeriesDbDialect;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;

public class DBDialectFactoryTest {

    @Test
    @Ignore
    public void testShouldReturnDb2MainframeDialectWhenCurrentDbIszOSDb2() throws Exception {
        DbDialectFactory factory = new DbDialectFactory();
        JdbcTemplate jdbcTemplate = new JdbcTemplate();
        String url = "jdbc:db2://mainframe_nonprod_db2.homeoffice.anfcorp.com:446/DBAU:currentSchema=MAMAHAW;";
        String username = "mamahaw";
        String password = "";
        Class.forName("com.ibm.db2.jcc.DB2Driver");
        DataSource dataSource = new SingleConnectionDataSource(url, username, password, true);
        jdbcTemplate.setDataSource(dataSource);
        factory.setJdbcTemplate(jdbcTemplate);
        factory.setBeanFactory(stubbedBeanFactory());
        Assert.assertTrue("should be db2 main frame", factory.getObject() instanceof Db2zSeriesDbDialect);
    }

    private BeanFactory stubbedBeanFactory() {
        return new BeanFactory() {

            public boolean containsBean(String name) {
                return false;
            }

            public String[] getAliases(String name) {
                return null;
            }

            public Object getBean(String name) throws BeansException {
                if (name.equals("db2Dialect")) {
                    return new Db2DbDialect();
                }
                if (name.equals("db2MainframeDialect")) {
                    return new Db2zSeriesDbDialect();
                }
                return null;
            }

            @SuppressWarnings("unchecked")
            public Object getBean(String name, Class requiredType) throws BeansException {
                return null;
            }

            public Object getBean(String name, Object... args) throws BeansException {
                return null;
            }

            @SuppressWarnings("unchecked")
            public Class getType(String name) throws NoSuchBeanDefinitionException {
                return null;
            }

            public boolean isPrototype(String name) throws NoSuchBeanDefinitionException {
                return false;
            }

            public boolean isSingleton(String name) throws NoSuchBeanDefinitionException {
                return false;
            }

            @SuppressWarnings("unchecked")
            public boolean isTypeMatch(String name, Class targetType) throws NoSuchBeanDefinitionException {
                return false;
            }
        };
    }
}
