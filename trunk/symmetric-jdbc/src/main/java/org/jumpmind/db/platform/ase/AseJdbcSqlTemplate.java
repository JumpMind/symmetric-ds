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
package org.jumpmind.db.platform.ase;

import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.jumpmind.db.platform.DatabaseInfo;
import org.jumpmind.db.platform.sqlanywhere.SqlAnywhereJdbcSqlTemplate;
import org.jumpmind.db.sql.ISqlTemplate;
import org.jumpmind.db.sql.ISqlTransaction;
import org.jumpmind.db.sql.JdbcSqlTemplate;
import org.jumpmind.db.sql.SqlTemplateSettings;
import org.jumpmind.db.sql.SymmetricLobHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.SqlTypeValue;
import org.springframework.jdbc.support.nativejdbc.NativeJdbcExtractor;

public class AseJdbcSqlTemplate extends JdbcSqlTemplate implements ISqlTemplate {

    static final Logger log = LoggerFactory.getLogger(AseJdbcSqlTemplate.class);

    private NativeJdbcExtractor nativeJdbcExtractor;

    public AseJdbcSqlTemplate(DataSource dataSource, SqlTemplateSettings settings,
            SymmetricLobHandler lobHandler, DatabaseInfo databaseInfo, NativeJdbcExtractor nativeJdbcExtractor) {
        super(dataSource, settings, lobHandler, databaseInfo);
        this.nativeJdbcExtractor = nativeJdbcExtractor;
        primaryKeyViolationCodes = new int[] {423,511,515,530,547,2601,2615,2714};
    }

    @Override
    public ISqlTransaction startSqlTransaction() {
        return new AseJdbcSqlTransaction(this);
    }

    @Override
    protected boolean allowsNullForIdentityColumn() {
        return false;
    }

    protected void setDecimalValue(PreparedStatement ps, int i, Object arg, int argType)
            throws SQLException {
        PreparedStatement nativeStatement = getNativeStmt(ps);
        if (nativeStatement != null
                && "com.sybase.jdbc4.jdbc.SybPreparedStatement".equals(nativeStatement.getClass()
                        .getName())) {
            Class<?> clazz = nativeStatement.getClass();
            Class<?>[] parameterTypes = new Class[] { int.class, BigDecimal.class, int.class,
                    int.class };
            Method method = null;
            try {
                method = clazz.getMethod("setBigDecimal", parameterTypes);
                BigDecimal value = (BigDecimal) arg;
                Object[] params = new Object[] { new Integer(i), value,
                        new Integer(value.precision()), new Integer(value.scale()) };
                method.invoke(nativeStatement, params);
            } catch (Exception e) {
                log.info("Can't find stmt.setBigDecimal(int,BigDecimal,int,int) method: "
                        + e.getMessage());
                return;
            }
        }
    }

    private PreparedStatement getNativeStmt(PreparedStatement ps) {
        PreparedStatement stmt = ps;
        try {
            stmt = nativeJdbcExtractor.getNativePreparedStatement(ps);
        } catch (SQLException ex) {
            log.debug("Could not find a native preparedstatement using {}", nativeJdbcExtractor
                    .getClass().getName());
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

}
