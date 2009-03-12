package org.jumpmind.symmetric.db;

import javax.sql.DataSource;

import junit.framework.TestCase;

import org.jumpmind.symmetric.db.db2.Db2DbDialect;
import org.jumpmind.symmetric.db.db2.ZSeriesDb2DbDialect;
import org.junit.Ignore;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;

public class DBDialectFactoryTest extends TestCase {

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
        assertTrue("should be db2 main frame", factory.getObject() instanceof ZSeriesDb2DbDialect);
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
                    return new ZSeriesDb2DbDialect();
                }
                return null;
            }

            @SuppressWarnings("unchecked")
            public Object getBean(String name, Class requiredType) throws BeansException {
                return null;
            }

            public Object getBean(String name, Object[] args) throws BeansException {
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
