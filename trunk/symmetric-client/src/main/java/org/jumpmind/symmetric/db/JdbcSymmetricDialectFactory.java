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
package org.jumpmind.symmetric.db;

import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.platform.ase.AseDatabasePlatform;
import org.jumpmind.db.platform.db2.Db2DatabasePlatform;
import org.jumpmind.db.platform.derby.DerbyDatabasePlatform;
import org.jumpmind.db.platform.firebird.FirebirdDatabasePlatform;
import org.jumpmind.db.platform.greenplum.GreenplumPlatform;
import org.jumpmind.db.platform.h2.H2DatabasePlatform;
import org.jumpmind.db.platform.hsqldb.HsqlDbDatabasePlatform;
import org.jumpmind.db.platform.hsqldb2.HsqlDb2DatabasePlatform;
import org.jumpmind.db.platform.informix.InformixDatabasePlatform;
import org.jumpmind.db.platform.interbase.InterbaseDatabasePlatform;
import org.jumpmind.db.platform.mariadb.MariaDBDatabasePlatform;
import org.jumpmind.db.platform.mssql.MsSqlDatabasePlatform;
import org.jumpmind.db.platform.mssql2000.MsSql2000DatabasePlatform;
import org.jumpmind.db.platform.mysql.MySqlDatabasePlatform;
import org.jumpmind.db.platform.oracle.OracleDatabasePlatform;
import org.jumpmind.db.platform.postgresql.PostgreSqlDatabasePlatform;
import org.jumpmind.db.platform.sqlanywhere.SqlAnywhereDatabasePlatform;
import org.jumpmind.db.platform.sqlite.SqliteDatabasePlatform;
import org.jumpmind.symmetric.db.ase.AseSymmetricDialect;
import org.jumpmind.symmetric.db.db2.Db2SymmetricDialect;
import org.jumpmind.symmetric.db.db2.Db2v9SymmetricDialect;
import org.jumpmind.symmetric.db.derby.DerbySymmetricDialect;
import org.jumpmind.symmetric.db.firebird.FirebirdSymmetricDialect;
import org.jumpmind.symmetric.db.h2.H2SymmetricDialect;
import org.jumpmind.symmetric.db.hsqldb.HsqlDbSymmetricDialect;
import org.jumpmind.symmetric.db.hsqldb2.HsqlDb2SymmetricDialect;
import org.jumpmind.symmetric.db.informix.InformixSymmetricDialect;
import org.jumpmind.symmetric.db.interbase.InterbaseSymmetricDialect;
import org.jumpmind.symmetric.db.mariadb.MariaDBSymmetricDialect;
import org.jumpmind.symmetric.db.mssql.MsSqlSymmetricDialect;
import org.jumpmind.symmetric.db.mssql2000.MsSql2000SymmetricDialect;
import org.jumpmind.symmetric.db.mysql.MySqlSymmetricDialect;
import org.jumpmind.symmetric.db.oracle.OracleSymmetricDialect;
import org.jumpmind.symmetric.db.postgresql.GreenplumSymmetricDialect;
import org.jumpmind.symmetric.db.postgresql.PostgreSqlSymmetricDialect;
import org.jumpmind.symmetric.db.sqlanywhere.SqlAnywhereSymmetricDialect;
import org.jumpmind.symmetric.db.sqlite.SqliteSymmetricDialect;
import org.jumpmind.symmetric.service.IParameterService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory class that is responsible for creating the appropriate
 * {@link ISymmetricDialect} for the configured database.
 */
public class JdbcSymmetricDialectFactory {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    private IParameterService parameterService;

    private IDatabasePlatform platform;

    public JdbcSymmetricDialectFactory(IParameterService parameterService, IDatabasePlatform platform) {
        this.parameterService = parameterService;
        this.platform = platform;
    }

    public ISymmetricDialect create() {

        AbstractSymmetricDialect dialect = null;

        if (platform instanceof MariaDBDatabasePlatform) {
            dialect = new MariaDBSymmetricDialect(parameterService, platform);
        } else if (platform instanceof MySqlDatabasePlatform) {
                dialect = new MySqlSymmetricDialect(parameterService, platform);
        } else if (platform instanceof OracleDatabasePlatform) {
            dialect = new OracleSymmetricDialect(parameterService, platform);
        } else if (platform instanceof MsSql2000DatabasePlatform) {
            dialect = new MsSql2000SymmetricDialect(parameterService, platform);
        } else if (platform instanceof MsSqlDatabasePlatform) {
            dialect = new MsSqlSymmetricDialect(parameterService, platform);
        } else if (platform instanceof GreenplumPlatform) {
            dialect = new GreenplumSymmetricDialect(parameterService, platform);
        } else if (platform instanceof PostgreSqlDatabasePlatform) {
            dialect = new PostgreSqlSymmetricDialect(parameterService, platform);
        } else if (platform instanceof DerbyDatabasePlatform) {
            dialect = new DerbySymmetricDialect(parameterService, platform);
        } else if (platform instanceof H2DatabasePlatform) {
            dialect = new H2SymmetricDialect(parameterService, platform);
        } else if (platform instanceof HsqlDbDatabasePlatform) {
            dialect = new HsqlDbSymmetricDialect(parameterService, platform);
        } else if (platform instanceof HsqlDb2DatabasePlatform) {
            dialect = new HsqlDb2SymmetricDialect(parameterService, platform);
        } else if (platform instanceof InformixDatabasePlatform) {
            dialect = new InformixSymmetricDialect(parameterService, platform);
        } else if (platform instanceof Db2DatabasePlatform) {
            int dbMajorVersion = platform.getSqlTemplate().getDatabaseMajorVersion();
            int dbMinorVersion = platform.getSqlTemplate().getDatabaseMinorVersion();
            if (dbMajorVersion < 9 || (dbMajorVersion == 9 && dbMinorVersion < 5)) {
                dialect = new Db2SymmetricDialect(parameterService, platform);
            } else {
                dialect = new Db2v9SymmetricDialect(parameterService, platform);
            }
        } else if (platform instanceof FirebirdDatabasePlatform) {
            dialect = new FirebirdSymmetricDialect(parameterService, platform);
        } else if (platform instanceof AseDatabasePlatform) {
            dialect = new AseSymmetricDialect(parameterService, platform);
        } else if (platform instanceof SqlAnywhereDatabasePlatform) {
            dialect = new SqlAnywhereSymmetricDialect(parameterService, platform);
        } else if (platform instanceof InterbaseDatabasePlatform) {
            dialect = new InterbaseSymmetricDialect(parameterService, platform);
        } else if (platform instanceof SqliteDatabasePlatform) {
            dialect = new SqliteSymmetricDialect(parameterService, platform);
        } else {
            throw new DbNotSupportedException();
        }
        return dialect;
    }

}