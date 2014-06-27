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

package org.jumpmind.symmetric.service.impl;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.ISqlTemplate;
import org.jumpmind.db.sql.ISqlTransaction;
import org.jumpmind.symmetric.common.TableConstants;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.model.IncomingBatch;
import org.jumpmind.symmetric.model.OutgoingBatch;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.service.IService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract public class AbstractService implements IService {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    protected IParameterService parameterService;

    protected ISymmetricDialect symmetricDialect;

    protected ISqlTemplate sqlTemplate;

    protected IDatabasePlatform platform;

    protected String tablePrefix;

    private ISqlMap sqlMap;

    public AbstractService(IParameterService parameterService, ISymmetricDialect symmetricDialect) {
        this.symmetricDialect = symmetricDialect;
        this.parameterService = parameterService;
        this.tablePrefix = parameterService.getTablePrefix();
        this.platform = symmetricDialect.getPlatform();
        this.sqlTemplate = symmetricDialect.getPlatform().getSqlTemplate();
    }

    protected void setSqlMap(ISqlMap sqlMap) {
        this.sqlMap = sqlMap;
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

    protected Map<String, String> createSqlReplacementTokens() {
        Map<String, String> replacementTokens = createSqlReplacementTokens(this.tablePrefix, symmetricDialect.getPlatform()
                .getDatabaseInfo().getDelimiterToken());
        replacementTokens.putAll(symmetricDialect.getSqlReplacementTokens());
        return replacementTokens;
    }
    
    protected static Map<String, String> createSqlReplacementTokens(String tablePrefix,
            String quotedIdentifier) {
        Map<String, String> map = new HashMap<String, String>();
        List<String> tables = TableConstants.getTablesWithoutPrefix();
        for (String table : tables) {
            map.put(table, String.format("%s%s%s", tablePrefix,
                    StringUtils.isNotBlank(tablePrefix) ? "_" : "", table));
        }
        return map;
    }

    public String getSql(String... keys) {
        if (sqlMap != null) {
            return sqlMap.getSql(keys);
        } else {
            return null;
        }
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

    protected String getRootMessage(Exception ex) {
        Throwable cause = ExceptionUtils.getRootCause(ex);
        if (cause == null) {
            cause = ex;
        }
        return cause.getMessage();
    }

    protected boolean isCalledFromSymmetricAdminTool() {
        boolean adminTool = false;
        StackTraceElement[] trace = Thread.currentThread().getStackTrace();
        for (StackTraceElement stackTraceElement : trace) {
            adminTool |= stackTraceElement.getClassName().equals(
                    "org.jumpmind.symmetric.SymmetricAdmin");
        }
        return adminTool;
    }
    
    protected String buildBatchWhere(List<String> nodeIds, List<String> channels,
            List<?> statuses) {
        boolean containsErrorStatus = statuses.contains(OutgoingBatch.Status.ER)
                || statuses.contains(IncomingBatch.Status.ER);
        boolean containsIgnoreStatus = statuses.contains(OutgoingBatch.Status.IG)
                || statuses.contains(IncomingBatch.Status.IG);

        StringBuilder where = new StringBuilder();
        boolean needsAnd = false;
        if (nodeIds.size() > 0) {
            where.append("node_id in (:NODES)");
            needsAnd = true;
        }
        if (channels.size() > 0) {
            if (needsAnd) {
                where.append(" and ");
            }
            where.append("channel_id in (:CHANNELS)");
            needsAnd = true;
        }
        if (statuses.size() > 0) {
            if (needsAnd) {
                where.append(" and ");
            }
            where.append("(status in (:STATUSES)");
            
            if (containsErrorStatus) {
                where.append(" or error_flag = 1 ");   
            }
            
            if (containsIgnoreStatus) {
                where.append(" or ignore_count > 0 ");   
            }
            
            where.append(")");

            needsAnd = true;
        }
        
        if (where.length() > 0) {
            where.insert(0, " where ");
        }
        return where.toString();
    }
    

}