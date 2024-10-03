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
package org.jumpmind.db.platform;

import org.jumpmind.db.platform.ase.AseDdlBuilder;
import org.jumpmind.db.platform.db2.Db2DdlBuilder;
import org.jumpmind.db.platform.derby.DerbyDdlBuilder;
import org.jumpmind.db.platform.firebird.FirebirdDdlBuilder;
import org.jumpmind.db.platform.firebird.FirebirdDialect1DdlBuilder;
import org.jumpmind.db.platform.greenplum.GreenplumDdlBuilder;
import org.jumpmind.db.platform.h2.H2DdlBuilder;
import org.jumpmind.db.platform.hbase.HbaseDdlBuilder;
import org.jumpmind.db.platform.hsqldb.HsqlDbDdlBuilder;
import org.jumpmind.db.platform.hsqldb2.HsqlDb2DdlBuilder;
import org.jumpmind.db.platform.informix.InformixDdlBuilder;
import org.jumpmind.db.platform.interbase.InterbaseDdlBuilder;
import org.jumpmind.db.platform.mssql.MsSql2000DdlBuilder;
import org.jumpmind.db.platform.mssql.MsSql2005DdlBuilder;
import org.jumpmind.db.platform.mssql.MsSql2008DdlBuilder;
import org.jumpmind.db.platform.mssql.MsSql2016DdlBuilder;
import org.jumpmind.db.platform.mysql.MySqlDdlBuilder;
import org.jumpmind.db.platform.nuodb.NuoDbDdlBuilder;
import org.jumpmind.db.platform.oracle.Oracle122DdlBuilder;
import org.jumpmind.db.platform.oracle.Oracle23DdlBuilder;
import org.jumpmind.db.platform.oracle.OracleDdlBuilder;
import org.jumpmind.db.platform.postgresql.PostgreSqlDdlBuilder;
import org.jumpmind.db.platform.postgresql.PostgreSqlDdlBuilder95;
import org.jumpmind.db.platform.redshift.RedshiftDdlBuilder;
import org.jumpmind.db.platform.sqlanywhere.SqlAnywhereDdlBuilder;
import org.jumpmind.db.platform.sqlite.SqliteDdlBuilder;
import org.jumpmind.db.platform.voltdb.VoltDbDdlBuilder;
import org.jumpmind.util.AppUtils;

/**
 * Factory that creates {@link IDdlBuilder} from {@link DatabaseNamesConstants} values.
 */
public class DdlBuilderFactory implements IDdlBuilderFactory {
    protected static IDdlBuilderFactory instance;

    protected DdlBuilderFactory() {
    }

    public static synchronized IDdlBuilderFactory getInstance() {
        if (instance == null) {
            instance = AppUtils.newInstance(IDdlBuilderFactory.class, DdlBuilderFactory.class);
        }
        return instance;
    }

    public IDdlBuilder create(String databaseName) {
        if (DatabaseNamesConstants.DB2.equals(databaseName)) {
            return new Db2DdlBuilder();
        } else if (DatabaseNamesConstants.DERBY.equalsIgnoreCase(databaseName)) {
            return new DerbyDdlBuilder();
        } else if (DatabaseNamesConstants.FIREBIRD.equalsIgnoreCase(databaseName)) {
            return new FirebirdDdlBuilder();
        } else if (DatabaseNamesConstants.FIREBIRD_DIALECT1.equalsIgnoreCase(databaseName)) {
            return new FirebirdDialect1DdlBuilder();
        } else if (DatabaseNamesConstants.GREENPLUM.equalsIgnoreCase(databaseName)) {
            return new GreenplumDdlBuilder();
        } else if (DatabaseNamesConstants.H2.equalsIgnoreCase(databaseName)) {
            return new H2DdlBuilder();
        } else if (DatabaseNamesConstants.HSQLDB.equalsIgnoreCase(databaseName)) {
            return new HsqlDbDdlBuilder();
        } else if (DatabaseNamesConstants.HSQLDB2.equalsIgnoreCase(databaseName)) {
            return new HsqlDb2DdlBuilder();
        } else if (DatabaseNamesConstants.INFORMIX.equalsIgnoreCase(databaseName)) {
            return new InformixDdlBuilder();
        } else if (DatabaseNamesConstants.INTERBASE.equalsIgnoreCase(databaseName)) {
            return new InterbaseDdlBuilder();
        } else if (DatabaseNamesConstants.MSSQL2000.equalsIgnoreCase(databaseName)) {
            return new MsSql2000DdlBuilder();
        } else if (DatabaseNamesConstants.MSSQL2005.equalsIgnoreCase(databaseName)) {
            return new MsSql2005DdlBuilder();
        } else if (DatabaseNamesConstants.MSSQL2008.equalsIgnoreCase(databaseName)) {
            return new MsSql2008DdlBuilder();
        } else if (DatabaseNamesConstants.MSSQL2016.equalsIgnoreCase(databaseName)) {
            return new MsSql2016DdlBuilder();
        } else if (DatabaseNamesConstants.MYSQL.equalsIgnoreCase(databaseName)) {
            return new MySqlDdlBuilder();
        } else if (DatabaseNamesConstants.ORACLE.equalsIgnoreCase(databaseName)) {
            return new OracleDdlBuilder();
        } else if (DatabaseNamesConstants.ORACLE122.equalsIgnoreCase(databaseName)) {
            return new Oracle122DdlBuilder();
        } else if (DatabaseNamesConstants.ORACLE23.equalsIgnoreCase(databaseName)) {
            return new Oracle23DdlBuilder();
        } else if (DatabaseNamesConstants.POSTGRESQL.equalsIgnoreCase(databaseName)) {
            return new PostgreSqlDdlBuilder();
        } else if (DatabaseNamesConstants.POSTGRESQL95.equalsIgnoreCase(databaseName)) {
            return new PostgreSqlDdlBuilder95();
        } else if (DatabaseNamesConstants.SQLITE.equalsIgnoreCase(databaseName)) {
            return new SqliteDdlBuilder();
        } else if (DatabaseNamesConstants.ASE.equalsIgnoreCase(databaseName)) {
            return new AseDdlBuilder();
        } else if (DatabaseNamesConstants.SQLANYWHERE.equalsIgnoreCase(databaseName)) {
            return new SqlAnywhereDdlBuilder();
        } else if (DatabaseNamesConstants.REDSHIFT.equalsIgnoreCase(databaseName)) {
            return new RedshiftDdlBuilder();
        } else if (DatabaseNamesConstants.VOLTDB.equalsIgnoreCase(databaseName)) {
            return new VoltDbDdlBuilder();
        } else if (DatabaseNamesConstants.NUODB.equalsIgnoreCase(databaseName)) {
            return new NuoDbDdlBuilder();
        } else if (DatabaseNamesConstants.HBASE.equalsIgnoreCase(databaseName)) {
            return new HbaseDdlBuilder();
        } else {
            return null;
        }
    }
}
