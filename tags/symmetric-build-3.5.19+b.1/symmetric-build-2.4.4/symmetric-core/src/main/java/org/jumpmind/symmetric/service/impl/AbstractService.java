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


package org.jumpmind.symmetric.service.impl;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.jumpmind.symmetric.common.logging.ILog;
import org.jumpmind.symmetric.common.logging.LogFactory;
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.service.IService;
import org.jumpmind.symmetric.service.ISqlProvider;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.simple.SimpleJdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;

abstract public class AbstractService implements IService, ISqlProvider {

    protected ILog log = LogFactory.getLog(getClass());

    protected IParameterService parameterService;

    protected JdbcTemplate jdbcTemplate;

    protected TransactionTemplate newTransactionTemplate;

    protected DataSource dataSource;

    protected IDbDialect dbDialect;

    protected String tablePrefix;

    private Map<String, String> sql;

    public void setJdbcTemplate(JdbcTemplate jdbc) {
        this.jdbcTemplate = jdbc;
    }
    
    public JdbcTemplate getJdbcTemplate() {
        return jdbcTemplate;
    }

    protected SimpleJdbcTemplate getSimpleTemplate() {
        return new SimpleJdbcTemplate(jdbcTemplate);
    }

    synchronized public void synchronize(Runnable runnable) {
        runnable.run();
    }
    
    protected boolean isSet(Object value) {
        if (value != null && value.toString().equals("1")) {
            return true;
        } else {
            return false;
        }
    }

    @SuppressWarnings("unchecked")
    protected SQLException unwrapSqlException(Throwable e) {
        List<Throwable> exs = ExceptionUtils.getThrowableList(e);
        for (Throwable throwable : exs) {
            if (throwable instanceof SQLException) {
                return (SQLException) throwable;
            }
        }
        return null;
    }

    public void setSql(Map<String, String> sql) {
        this.sql = sql;
    }

    public String getSql(String... keys) {
        StringBuilder sqlBuffer = new StringBuilder();
        if (keys != null) {
            for (String key : keys) {
                if (key != null) {
                    String value = sql.get(key);
                    sqlBuffer.append(value == null ? key : value);
                }
            }

            if (dbDialect != null) {
               sqlBuffer = dbDialect.scrubSql(sqlBuffer);
            }
        }
        return sqlBuffer.toString();
    }

    public void setTablePrefix(String tablePrefix) {
        this.tablePrefix = tablePrefix;
    }

    public void setParameterService(IParameterService parameterService) {
        this.parameterService = parameterService;
    }
    
    public IParameterService getParameterService() {
        return parameterService;
    }

    public void setNewTransactionTemplate(TransactionTemplate transactionTemplate) {
        this.newTransactionTemplate = transactionTemplate;
    }

    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public void setDbDialect(IDbDialect dbDialect) {
        this.dbDialect = dbDialect;
    }
    
    public IDbDialect getDbDialect() {
        return dbDialect;
    }
    
    public String getTablePrefix() {
        return tablePrefix;
    }

}