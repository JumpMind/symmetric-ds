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

package org.jumpmind.symmetric.db;

import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.platform.db2.Db2Platform;
import org.jumpmind.db.platform.derby.DerbyPlatform;
import org.jumpmind.db.platform.firebird.FirebirdPlatform;
import org.jumpmind.db.platform.greenplum.GreenplumPlatform;
import org.jumpmind.db.platform.h2.H2Platform;
import org.jumpmind.db.platform.hsqldb.HsqlDbPlatform;
import org.jumpmind.db.platform.hsqldb2.HsqlDb2Platform;
import org.jumpmind.db.platform.informix.InformixPlatform;
import org.jumpmind.db.platform.interbase.InterbasePlatform;
import org.jumpmind.db.platform.mssql.MsSqlPlatform;
import org.jumpmind.db.platform.mysql.MySqlPlatform;
import org.jumpmind.db.platform.oracle.OraclePlatform;
import org.jumpmind.db.platform.postgresql.PostgreSqlPlatform;
import org.jumpmind.db.platform.sybase.SybasePlatform;
import org.jumpmind.log.Log;
import org.jumpmind.log.LogFactory;
import org.jumpmind.symmetric.db.db2.Db2SymmetricDialect;
import org.jumpmind.symmetric.db.db2.Db2v9SymmetricDialect;
import org.jumpmind.symmetric.db.derby.DerbySymmetricDialect;
import org.jumpmind.symmetric.db.firebird.FirebirdSymmetricDialect;
import org.jumpmind.symmetric.db.h2.H2SymmetricDialect;
import org.jumpmind.symmetric.db.hsqldb.HsqlDbSymmetricDialect;
import org.jumpmind.symmetric.db.hsqldb2.HsqlDb2SymmetricDialect;
import org.jumpmind.symmetric.db.informix.InformixSymmetricDialect;
import org.jumpmind.symmetric.db.interbase.InterbaseSymmetricDialect;
import org.jumpmind.symmetric.db.mssql.MsSqlSymmetricDialect;
import org.jumpmind.symmetric.db.mysql.MySqlSymmetricDialect;
import org.jumpmind.symmetric.db.oracle.OracleSymmetricDialect;
import org.jumpmind.symmetric.db.postgresql.GreenplumSymmetricDialect;
import org.jumpmind.symmetric.db.postgresql.PostgreSqlSymmetricDialect;
import org.jumpmind.symmetric.db.sybase.SybaseSymmetricDialect;
import org.jumpmind.symmetric.service.IParameterService;

/**
 * Factory class that is responsible for creating the appropriate
 * {@link ISymmetricDialect} for the configured database.
 */
public class JdbcSymmetricDialectFactory {

    protected Log log = LogFactory.getLog(getClass());

    private IParameterService parameterService;

    private IDatabasePlatform platform;

    public JdbcSymmetricDialectFactory(IParameterService parameterService, IDatabasePlatform platform,
            Log log) {
        this.parameterService = parameterService;
        this.platform = platform;
        this.log = log;
    }

    public ISymmetricDialect create() {

        AbstractSymmetricDialect dialect = null;

        if (platform instanceof MySqlPlatform) {
            dialect = new MySqlSymmetricDialect(parameterService, platform);
        } else if (platform instanceof OraclePlatform) {
            dialect = new OracleSymmetricDialect(parameterService, platform);
        } else if (platform instanceof MsSqlPlatform) {
            dialect = new MsSqlSymmetricDialect(parameterService, platform);
        } else if (platform instanceof GreenplumPlatform) {
            dialect = new GreenplumSymmetricDialect(parameterService, platform);
        } else if (platform instanceof PostgreSqlPlatform) {
            dialect = new PostgreSqlSymmetricDialect(parameterService, platform);
        } else if (platform instanceof DerbyPlatform) {
            dialect = new DerbySymmetricDialect(parameterService, platform);
        } else if (platform instanceof H2Platform) {
            dialect = new H2SymmetricDialect(parameterService, platform);
        } else if (platform instanceof HsqlDbPlatform) {
            dialect = new HsqlDbSymmetricDialect(parameterService, platform);
        } else if (platform instanceof HsqlDb2Platform) {
            dialect = new HsqlDb2SymmetricDialect(parameterService, platform);
        } else if (platform instanceof InformixPlatform) {
            dialect = new InformixSymmetricDialect(parameterService, platform);
        } else if (platform instanceof Db2Platform) {
            int dbMajorVersion = platform.getSqlTemplate().getDatabaseMajorVersion();
            int dbMinorVersion = platform.getSqlTemplate().getDatabaseMinorVersion();
            if (dbMajorVersion < 9 || (dbMajorVersion == 9 && dbMinorVersion < 5)) {
                dialect = new Db2SymmetricDialect(parameterService, platform);
            } else {
                dialect = new Db2v9SymmetricDialect(parameterService, platform);
            }
        } else if (platform instanceof FirebirdPlatform) {
            dialect = new FirebirdSymmetricDialect(parameterService, platform);
        } else if (platform instanceof SybasePlatform) {
            dialect = new SybaseSymmetricDialect(parameterService, platform);
        } else if (platform instanceof InterbasePlatform) {
            dialect = new InterbaseSymmetricDialect(parameterService, platform);
        } else {
            throw new DbNotSupportedException();
        }
        return dialect;
    }

}