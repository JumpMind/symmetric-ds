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
import org.jumpmind.db.platform.db2.Db2As400DatabasePlatform;
import org.jumpmind.db.platform.db2.Db2DatabasePlatform;
import org.jumpmind.db.platform.db2.Db2zOsDatabasePlatform;
import org.jumpmind.db.platform.derby.DerbyDatabasePlatform;
import org.jumpmind.db.platform.firebird.FirebirdDatabasePlatform;
import org.jumpmind.db.platform.greenplum.GreenplumPlatform;
import org.jumpmind.db.platform.h2.H2DatabasePlatform;
import org.jumpmind.db.platform.hsqldb.HsqlDbDatabasePlatform;
import org.jumpmind.db.platform.hsqldb2.HsqlDb2DatabasePlatform;
import org.jumpmind.db.platform.informix.InformixDatabasePlatform;
import org.jumpmind.db.platform.interbase.InterbaseDatabasePlatform;
import org.jumpmind.db.platform.mariadb.MariaDBDatabasePlatform;
import org.jumpmind.db.platform.mssql.MsSql2000DatabasePlatform;
import org.jumpmind.db.platform.mssql.MsSql2005DatabasePlatform;
import org.jumpmind.db.platform.mssql.MsSql2008DatabasePlatform;
import org.jumpmind.db.platform.mysql.MySqlDatabasePlatform;
import org.jumpmind.db.platform.nuodb.NuoDbDatabasePlatform;
import org.jumpmind.db.platform.oracle.OracleDatabasePlatform;
import org.jumpmind.db.platform.postgresql.PostgreSqlDatabasePlatform;
import org.jumpmind.db.platform.raima.RaimaDatabasePlatform;
import org.jumpmind.db.platform.redshift.RedshiftDatabasePlatform;
import org.jumpmind.db.platform.sqlanywhere.SqlAnywhereDatabasePlatform;
import org.jumpmind.db.platform.sqlite.SqliteDatabasePlatform;
import org.jumpmind.db.platform.tibero.TiberoDatabasePlatform;
import org.jumpmind.db.platform.voltdb.VoltDbDatabasePlatform;
import org.jumpmind.symmetric.db.ase.AseSymmetricDialect;
import org.jumpmind.symmetric.db.db2.Db2As400SymmetricDialect;
import org.jumpmind.symmetric.db.db2.Db2SymmetricDialect;
import org.jumpmind.symmetric.db.db2.Db2v9SymmetricDialect;
import org.jumpmind.symmetric.db.db2.Db2zOsSymmetricDialect;
import org.jumpmind.symmetric.db.derby.DerbySymmetricDialect;
import org.jumpmind.symmetric.db.firebird.Firebird20SymmetricDialect;
import org.jumpmind.symmetric.db.firebird.Firebird21SymmetricDialect;
import org.jumpmind.symmetric.db.firebird.FirebirdSymmetricDialect;
import org.jumpmind.symmetric.db.generic.GenericSymmetricDialect;
import org.jumpmind.symmetric.db.h2.H2SymmetricDialect;
import org.jumpmind.symmetric.db.hsqldb.HsqlDbSymmetricDialect;
import org.jumpmind.symmetric.db.hsqldb2.HsqlDb2SymmetricDialect;
import org.jumpmind.symmetric.db.informix.InformixSymmetricDialect;
import org.jumpmind.symmetric.db.interbase.InterbaseSymmetricDialect;
import org.jumpmind.symmetric.db.mariadb.MariaDBSymmetricDialect;
import org.jumpmind.symmetric.db.mssql.MsSql2008SymmetricDialect;
import org.jumpmind.symmetric.db.mssql.MsSqlSymmetricDialect;
import org.jumpmind.symmetric.db.mssql2000.MsSql2000SymmetricDialect;
import org.jumpmind.symmetric.db.mysql.MySqlSymmetricDialect;
import org.jumpmind.symmetric.db.nuodb.NuoDbSymmetricDialect;
import org.jumpmind.symmetric.db.oracle.OracleSymmetricDialect;
import org.jumpmind.symmetric.db.postgresql.GreenplumSymmetricDialect;
import org.jumpmind.symmetric.db.postgresql.PostgreSqlSymmetricDialect;
import org.jumpmind.symmetric.db.raima.RaimaSymmetricDialect;
import org.jumpmind.symmetric.db.redshift.RedshiftSymmetricDialect;
import org.jumpmind.symmetric.db.sqlanywhere.SqlAnywhereSymmetricDialect;
import org.jumpmind.symmetric.db.sqlite.SqliteJdbcSymmetricDialect;
import org.jumpmind.symmetric.db.tibero.TiberoSymmetricDialect;
import org.jumpmind.symmetric.db.voltdb.VoltDbSymmetricDialect;
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
        } else if (platform instanceof MsSql2008DatabasePlatform) {
            dialect = new MsSql2008SymmetricDialect(parameterService, platform);
        } else if (platform instanceof MsSql2005DatabasePlatform) {
            dialect = new MsSqlSymmetricDialect(parameterService, platform);
        } else if (platform instanceof MsSql2000DatabasePlatform) {
            dialect = new MsSql2000SymmetricDialect(parameterService, platform);
        } else if (platform instanceof GreenplumPlatform) {
            dialect = new GreenplumSymmetricDialect(parameterService, platform);
        } else if (platform instanceof RedshiftDatabasePlatform) {
            dialect = new RedshiftSymmetricDialect(parameterService, platform);
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
        } else if (platform instanceof Db2zOsDatabasePlatform) {
            dialect = new Db2zOsSymmetricDialect(parameterService, platform);
        } else if (platform instanceof Db2As400DatabasePlatform) {
            dialect = new Db2As400SymmetricDialect(parameterService, platform);
        } else if (platform instanceof Db2DatabasePlatform) {
            int dbMajorVersion = platform.getSqlTemplate().getDatabaseMajorVersion();
            int dbMinorVersion = platform.getSqlTemplate().getDatabaseMinorVersion();
            if (dbMajorVersion < 9 || (dbMajorVersion == 9 && dbMinorVersion < 5)) {
                dialect = new Db2SymmetricDialect(parameterService, platform);
            } else {
                dialect = new Db2v9SymmetricDialect(parameterService, platform);
            }
        } else if (platform instanceof FirebirdDatabasePlatform) {
            int dbMajorVersion = platform.getSqlTemplate().getDatabaseMajorVersion();
            int dbMinorVersion = platform.getSqlTemplate().getDatabaseMinorVersion();
            if (dbMajorVersion == 2 && dbMinorVersion == 0) {
                dialect = new Firebird20SymmetricDialect(parameterService, platform);
            } else if (dbMajorVersion == 2) {
                dialect = new Firebird21SymmetricDialect(parameterService, platform);
            } else {
                dialect = new FirebirdSymmetricDialect(parameterService, platform);
            }
        } else if (platform instanceof AseDatabasePlatform) {
            dialect = new AseSymmetricDialect(parameterService, platform);
        } else if (platform instanceof SqlAnywhereDatabasePlatform) {
            dialect = new SqlAnywhereSymmetricDialect(parameterService, platform);
        } else if (platform instanceof InterbaseDatabasePlatform) {
            dialect = new InterbaseSymmetricDialect(parameterService, platform);
        } else if (platform instanceof SqliteDatabasePlatform) {
            dialect = new SqliteJdbcSymmetricDialect(parameterService, platform);
        } else if (platform instanceof VoltDbDatabasePlatform) {
            dialect = new VoltDbSymmetricDialect(parameterService, platform);
        } else if (platform instanceof TiberoDatabasePlatform) {
            dialect = new TiberoSymmetricDialect(parameterService, platform);
        } else if (platform instanceof NuoDbDatabasePlatform){
            dialect = new NuoDbSymmetricDialect(parameterService, platform);
        } else if (platform instanceof RaimaDatabasePlatform){
            dialect = new RaimaSymmetricDialect(parameterService, platform);
        }else{
            dialect = new GenericSymmetricDialect(parameterService, platform);
        }
        return dialect;
    }

}