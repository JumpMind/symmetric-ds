/**
 * Licensed to JumpMind Inc under one or more contributor
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU General Public License, version 3.0 (GPLv3)
 * (the "License"); you may not use this file except in compliance
 * with the License.
 *
 * You should have received a copy of the GNU General Public License,
 * version 3.0 (GPLv3) along with this library; if not, see
 * <http://www.gnu.org/licenses/>.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.jumpmind.db.platform.sybase;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;

import javax.sql.DataSource;

import org.jumpmind.db.platform.DatabaseInfo;
import org.jumpmind.db.sql.ISqlTemplate;
import org.jumpmind.db.sql.JdbcSqlTemplate;
import org.jumpmind.db.sql.SqlTemplateSettings;
import org.jumpmind.db.sql.SymmetricLobHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.SqlTypeValue;
import org.springframework.jdbc.support.nativejdbc.NativeJdbcExtractor;

public class SybaseJdbcSqlTemplate extends JdbcSqlTemplate implements ISqlTemplate {

    static final Logger log = LoggerFactory.getLogger(SybaseJdbcSqlTemplate.class);

    private NativeJdbcExtractor nativeJdbcExtractor;
    
    int jdbcMajorVersion;

    public SybaseJdbcSqlTemplate(DataSource dataSource, SqlTemplateSettings settings,
            SymmetricLobHandler lobHandler, DatabaseInfo databaseInfo, NativeJdbcExtractor nativeJdbcExtractor) {
        super(dataSource, settings, lobHandler, databaseInfo);
        this.nativeJdbcExtractor = nativeJdbcExtractor;
        primaryKeyViolationCodes = new int[] {423,511,515,530,547,2601,2615,2714};
        foreignKeyViolationCodes = new int[] {546};
        Connection c = null;
        try {
            c = dataSource.getConnection();
            jdbcMajorVersion = c.getMetaData().getJDBCMajorVersion();
        } catch (SQLException ex) {
            jdbcMajorVersion = -1;
        } finally {
            close(c);
        }
    }

    @Override
    protected boolean allowsNullForIdentityColumn() {
        return false;
    }

    protected void setDecimalValue(PreparedStatement ps, int i, Object arg, int argType) throws SQLException {        
        if ((argType == Types.DECIMAL || argType == Types.NUMERIC) && arg != null && arg.equals("NaN")) {
            setNanOrNull(ps, i, arg, argType);
        } else {
            PreparedStatement nativeStatement = getNativeStmt(ps);
            if (nativeStatement != null && "com.sybase.jdbc4.jdbc.SybPreparedStatement".equals(nativeStatement.getClass().getName())) {
                Class<?> clazz = nativeStatement.getClass();
                Class<?>[] parameterTypes = new Class[] { int.class, BigDecimal.class, int.class, int.class };
                BigDecimal value = null;
                if (arg instanceof BigDecimal) {
                    value = (BigDecimal) arg;
                } else if (arg instanceof Boolean) {
                    // Leave the BigDecimal null for a Boolean
                } else if (arg != null) {
                    value = new BigDecimal(arg.toString());
                }

                int precision = 1;
                int scale = 0;
                if (value != null) {
                    scale = value.scale();
                    precision = value.precision();
                    if (precision < scale) {
                        precision = scale + 1;
                    }

                    if (precision > 127) {
                        precision = 127;

                        if (scale > 127) {
                            scale = 126;
                        }
                    }
                }

                Object[] params = new Object[] { new Integer(i), value, new Integer(precision), new Integer(scale) };
                try {
                    if (arg instanceof Long) {
                        params = new Object[] { new Integer(i), new Long(arg.toString()) };
                        parameterTypes = new Class[] { int.class, long.class };
                        Method method = clazz.getMethod("setLong", parameterTypes);
                        method.invoke(nativeStatement, params);
                    }
                    else if (arg instanceof Integer) {
                        params = new Object[] { new Integer(i), new Integer(arg.toString()) };
                        parameterTypes = new Class[] { int.class, int.class };
                        Method method = clazz.getMethod("setInt", parameterTypes);
                        method.invoke(nativeStatement, params);
                    } else if (arg instanceof Boolean) {
                        Integer intValue = ((Boolean)arg) ? Integer.valueOf(1) : Integer.valueOf(0);
                        params = new Object[] { new Integer(i), intValue };
                        parameterTypes = new Class[] { int.class, int.class };
                        Method method = clazz.getMethod("setInt", parameterTypes);
                        method.invoke(nativeStatement, params);                        
                    }
                    else {
                        Method method = clazz.getMethod("setBigDecimal", parameterTypes);
                        method.invoke(nativeStatement, params);
                    }
                } catch (Throwable e) {
                    if (e instanceof InvocationTargetException) {
                        e = ((InvocationTargetException) e).getTargetException();
                    }
                    log.warn(String.format("Error calling the Sybase stmt.setBigDecimal(%s) method", Arrays.toString(params)), e);
                    super.setDecimalValue(ps, i, arg, argType);
                }
            } else {
                super.setDecimalValue(ps, i, arg, argType);
            }
        }
    }
 

    private PreparedStatement getNativeStmt(PreparedStatement ps) {
        PreparedStatement stmt = ps;
        try {
            stmt = nativeJdbcExtractor.getNativePreparedStatement(ps);
        } catch (SQLException ex) {
            log.warn("Could not find a native preparedstatement using {}", nativeJdbcExtractor
                    .getClass().getName(), ex);
        }
        return stmt;
    }

    @Override
    public void setValues(PreparedStatement ps, Object[] args)
            throws SQLException {
        super.setValues(ps, args);
        if (args != null && args.length > 0) {
            int[] argTypes = new int[args.length];
            for (int i = 0; i < argTypes.length; i++) {
                argTypes[i] = SqlTypeValue.TYPE_UNKNOWN;
            }
            setValues(ps, args, argTypes, getLobHandler().getDefaultHandler());
        }
    }
    
    public boolean supportsGetGeneratedKeys() {
        return jdbcMajorVersion >= 4;
    }

    protected String getSelectLastInsertIdSql(String sequenceName) {
        return "select @@identity";
    }

}
