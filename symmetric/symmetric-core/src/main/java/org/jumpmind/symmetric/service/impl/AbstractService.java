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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.jumpmind.db.sql.ISqlMap;
import org.jumpmind.db.sql.ISqlTemplate;
import org.jumpmind.db.sql.ISqlTransaction;
import org.jumpmind.log.Log;
import org.jumpmind.log.LogFactory;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.service.IService;

abstract public class AbstractService implements IService {

    protected Log log = LogFactory.getLog(getClass());

    protected IParameterService parameterService;

    protected ISymmetricDialect symmetricDialect;
    
    protected ISqlTemplate sqlTemplate;

    protected String tablePrefix;
    
    private ISqlMap sqlMap;
    
    public AbstractService(Log log, IParameterService parameterService, ISymmetricDialect symmetricDialect) {
       this.log = log;
       this.symmetricDialect = symmetricDialect;
       this.parameterService = parameterService;
       this.tablePrefix = parameterService.getTablePrefix();
       this.sqlTemplate = symmetricDialect.getPlatform().getSqlTemplate();
    }
    
    public ISqlTemplate getJdbcTemplate() {
        return symmetricDialect.getPlatform().getSqlTemplate();
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
    
    final protected ISqlMap getSqlMap() {
        if (sqlMap == null) {
            sqlMap = createSqlMap();
        }
        return sqlMap;
    }
    
    abstract protected ISqlMap createSqlMap();
    
    protected Map<String,String> createSqlReplacementTokens() {
        return createSqlReplacementTokens(this.tablePrefix);
    }    
    
    protected static Map<String,String> createSqlReplacementTokens(String tablePrefix) {
        Map<String,String> map = new HashMap<String, String>();
        map.put("prefixName", tablePrefix);
        return map;
    } 

    public String getSql(String... keys) {
        return getSqlMap().getSql(keys);
    }

    public IParameterService getParameterService() {
        return parameterService;
    }
    
    public ISymmetricDialect getSymmetricDialect() {
        return symmetricDialect;
    }
    
    public String getTablePrefix() {
        return tablePrefix;
    }
    
    protected void close(ISqlTransaction transaction) {
        if (transaction != null) {
            transaction.close();
        }
    }

}