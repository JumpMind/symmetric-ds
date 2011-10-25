package org.jumpmind.symmetric.ddl.platform;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import junit.framework.TestCase;

import org.jumpmind.symmetric.ddl.PlatformUtils;
import org.jumpmind.symmetric.ddl.platform.cloudscape.CloudscapePlatform;
import org.jumpmind.symmetric.ddl.platform.db2.Db2Platform;
import org.jumpmind.symmetric.ddl.platform.derby.DerbyPlatform;
import org.jumpmind.symmetric.ddl.platform.firebird.FirebirdPlatform;
import org.jumpmind.symmetric.ddl.platform.hsqldb.HsqlDbPlatform;
import org.jumpmind.symmetric.ddl.platform.interbase.InterbasePlatform;
import org.jumpmind.symmetric.ddl.platform.mssql.MSSqlPlatform;
import org.jumpmind.symmetric.ddl.platform.mysql.MySqlPlatform;
import org.jumpmind.symmetric.ddl.platform.oracle.Oracle8Platform;
import org.jumpmind.symmetric.ddl.platform.postgresql.PostgreSqlPlatform;
import org.jumpmind.symmetric.ddl.platform.sybase.SybasePlatform;

/*
 * Tests the {@link org.jumpmind.symmetric.ddl.PlatformUtils} class.
 */
public class TestPlatformUtils extends TestCase
{

    /*
     * {@inheritDoc}
     */
    protected void setUp() throws Exception
    {      
    }

    /*
     * {@inheritDoc}
     */
    protected void tearDown() throws Exception
    {
    }

    /*
     * Tests the determination of the Db2 platform via its JDBC drivers.
     */
    public void testDb2Driver()
    {
        assertEquals(Db2Platform.DATABASENAME,
                     PlatformUtils.determineDatabaseType("com.ibm.db2.jcc.DB2Driver", null));
        assertEquals(Db2Platform.DATABASENAME,
                     PlatformUtils.determineDatabaseType("COM.ibm.db2os390.sqlj.jdbc.DB2SQLJDriver", null));
        assertEquals(Db2Platform.DATABASENAME,
                     PlatformUtils.determineDatabaseType("COM.ibm.db2.jdbc.app.DB2Driver", null));
        // DataDirect Connect
        assertEquals(Db2Platform.DATABASENAME,
                     PlatformUtils.determineDatabaseType("com.ddtek.jdbc.db2.DB2Driver", null));
        // i-net
        assertEquals(Db2Platform.DATABASENAME,
                     PlatformUtils.determineDatabaseType("com.inet.drda.DRDADriver", null));
    }

    /*
     * Tests the determination of the Db2 platform via JDBC connection urls.
     */
    public void testDb2Url()
    {
        assertEquals(Db2Platform.DATABASENAME,
                     PlatformUtils.determineDatabaseType(null, "jdbc:db2://sysmvs1.stl.ibm.com:5021/san_jose"));
        assertEquals(Db2Platform.DATABASENAME,
                     PlatformUtils.determineDatabaseType(null, "jdbc:db2os390://sysmvs1.stl.ibm.com:5021/san_jose"));
        assertEquals(Db2Platform.DATABASENAME,
                     PlatformUtils.determineDatabaseType(null, "jdbc:db2os390sqlj://sysmvs1.stl.ibm.com:5021/san_jose"));
        // DataDirect Connect
        assertEquals(Db2Platform.DATABASENAME,
                     PlatformUtils.determineDatabaseType(null, "jdbc:datadirect:db2://server1:50000;DatabaseName=jdbc;User=test;Password=secret"));
        // i-net
        assertEquals(Db2Platform.DATABASENAME,
                     PlatformUtils.determineDatabaseType(null, "jdbc:inetdb2://server1:50000"));
    }

    /*
     * Tests the determination of the Cloudscape platform via JDBC connection urls.
     */
    public void testCloudscapeUrl()
    {
        assertEquals(CloudscapePlatform.DATABASENAME,
                     PlatformUtils.determineDatabaseType(null, "jdbc:db2j:net:database"));
        assertEquals(CloudscapePlatform.DATABASENAME,
                     PlatformUtils.determineDatabaseType(null, "jdbc:cloudscape:net:database"));
    }

    /*
     * Tests the determination of the Derby platform via its JDBC drivers.
     */
    public void testDerbyDriver()
    {
        assertEquals(DerbyPlatform.DATABASENAME,
                     PlatformUtils.determineDatabaseType("org.apache.derby.jdbc.ClientDriver", null));
        assertEquals(DerbyPlatform.DATABASENAME,
                     PlatformUtils.determineDatabaseType("org.apache.derby.jdbc.EmbeddedDriver", null));
    }

    /*
     * Tests the determination of the Derby platform via JDBC connection urls.
     */
    public void testDerbyUrl()
    {
        assertEquals(DerbyPlatform.DATABASENAME,
                     PlatformUtils.determineDatabaseType(null, "jdbc:derby:sample"));
    }

    /*
     * Tests the determination of the Firebird platform via its JDBC driver.
     */
    public void testFirebirdDriver()
    {
        assertEquals(FirebirdPlatform.DATABASENAME,
                     PlatformUtils.determineDatabaseType("org.firebirdsql.jdbc.FBDriver", null));
    }

    /*
     * Tests the determination of the Firebird platform via JDBC connection urls.
     */
    public void testFirebirdUrl()
    {
        assertEquals(FirebirdPlatform.DATABASENAME,
                     PlatformUtils.determineDatabaseType(null, "jdbc:firebirdsql://localhost:8080/path/to/db.fdb"));
        assertEquals(FirebirdPlatform.DATABASENAME,
                    PlatformUtils.determineDatabaseType(null, "jdbc:firebirdsql:native:localhost/8080:/path/to/db.fdb"));
        assertEquals(FirebirdPlatform.DATABASENAME,
                     PlatformUtils.determineDatabaseType(null, "jdbc:firebirdsql:local://localhost:8080:/path/to/db.fdb"));
        assertEquals(FirebirdPlatform.DATABASENAME,
                     PlatformUtils.determineDatabaseType(null, "jdbc:firebirdsql:embedded:localhost/8080:/path/to/db.fdb"));
    }

    /*
     * Tests the determination of the HsqlDb platform via its JDBC driver.
     */
    public void testHsqldbDriver()
    {
        assertEquals(HsqlDbPlatform.DATABASENAME,
                     PlatformUtils.determineDatabaseType("org.hsqldb.jdbcDriver", null));
    }

    /*
     * Tests the determination of the HsqlDb platform via JDBC connection urls.
     */
    public void testHsqldbUrl()
    {
        assertEquals(HsqlDbPlatform.DATABASENAME,
                     PlatformUtils.determineDatabaseType(null, "jdbc:hsqldb:/opt/db/testdb"));
    }

    /*
     * Tests the determination of the Interbase platform via its JDBC driver.
     */
    public void testInterbaseDriver()
    {
        assertEquals(InterbasePlatform.DATABASENAME,
                     PlatformUtils.determineDatabaseType("interbase.interclient.Driver", null));
    }

    /*
     * Tests the determination of the Interbase platform via JDBC connection urls.
     */
    public void testInterbaseUrl()
    {
        assertEquals(InterbasePlatform.DATABASENAME,
                     PlatformUtils.determineDatabaseType(null, "jdbc:interbase://localhost/e:/testbed/database/employee.gdb"));
    }

    /*
     * Tests the determination of the Microsoft Sql Server platform via its JDBC drivers.
     */
    public void testMsSqlDriver()
    {
        assertEquals(MSSqlPlatform.DATABASENAME,
                     PlatformUtils.determineDatabaseType("com.microsoft.jdbc.sqlserver.SQLServerDriver", null));
        // DataDirect Connect
        assertEquals(MSSqlPlatform.DATABASENAME,
                     PlatformUtils.determineDatabaseType("com.ddtek.jdbc.sqlserver.SQLServerDriver", null));
        // JNetDirect JSQLConnect
        assertEquals(MSSqlPlatform.DATABASENAME,
                     PlatformUtils.determineDatabaseType("com.jnetdirect.jsql.JSQLDriver", null));
        // i-net
        assertEquals(MSSqlPlatform.DATABASENAME,
                     PlatformUtils.determineDatabaseType("com.inet.tds.TdsDriver", null));
    }

    /*
     * Tests the determination of the Microsoft Sql Server platform via JDBC connection urls.
     */
    public void testMsSqlUrl()
    {
        assertEquals(MSSqlPlatform.DATABASENAME,
                     PlatformUtils.determineDatabaseType(null, "jdbc:microsoft:sqlserver://localhost:1433"));
        // DataDirect Connect
        assertEquals(MSSqlPlatform.DATABASENAME,
                     PlatformUtils.determineDatabaseType(null, "jdbc:datadirect:sqlserver://server1:1433;User=test;Password=secret"));
        // JNetDirect JSQLConnect
        assertEquals(MSSqlPlatform.DATABASENAME,
                     PlatformUtils.determineDatabaseType(null, "jdbc:JSQLConnect://localhost/database=master/user=sa/sqlVersion=6"));
        // i-net
        assertEquals(MSSqlPlatform.DATABASENAME,
                     PlatformUtils.determineDatabaseType(null, "jdbc:inetdae:210.1.164.19:1433"));
        assertEquals(MSSqlPlatform.DATABASENAME,
                     PlatformUtils.determineDatabaseType(null, "jdbc:inetdae6:[2002:d201:a413::d201:a413]:1433"));
        assertEquals(MSSqlPlatform.DATABASENAME,
                     PlatformUtils.determineDatabaseType(null, "jdbc:inetdae7:localHost:1433"));
        assertEquals(MSSqlPlatform.DATABASENAME,
                     PlatformUtils.determineDatabaseType(null, "jdbc:inetdae7a://MyServer/pipe/sql/query"));
        assertEquals(MSSqlPlatform.DATABASENAME,
                     PlatformUtils.determineDatabaseType(null, "jdbc:inetpool:inetdae:210.1.164.19:1433"));
        assertEquals(MSSqlPlatform.DATABASENAME,
                     PlatformUtils.determineDatabaseType(null, "jdbc:inetpool:inetdae6:[2002:d201:a413::d201:a413]:1433"));
        assertEquals(MSSqlPlatform.DATABASENAME,
                     PlatformUtils.determineDatabaseType(null, "jdbc:inetpool:inetdae7:localHost:1433"));
        assertEquals(MSSqlPlatform.DATABASENAME,
                     PlatformUtils.determineDatabaseType(null, "jdbc:inetpool:inetdae7a://MyServer/pipe/sql/query"));
        assertEquals(MSSqlPlatform.DATABASENAME,
                     PlatformUtils.determineDatabaseType(null, "jdbc:inetpool:jdbc:inetdae:210.1.164.19:1433"));
        assertEquals(MSSqlPlatform.DATABASENAME,
                     PlatformUtils.determineDatabaseType(null, "jdbc:inetpool:jdbc:inetdae6:[2002:d201:a413::d201:a413]:1433"));
        assertEquals(MSSqlPlatform.DATABASENAME,
                     PlatformUtils.determineDatabaseType(null, "jdbc:inetpool:jdbc:inetdae7:localHost:1433"));
        assertEquals(MSSqlPlatform.DATABASENAME,
                     PlatformUtils.determineDatabaseType(null, "jdbc:inetpool:jdbc:inetdae7a://MyServer/pipe/sql/query"));
        // jTDS
        assertEquals(MSSqlPlatform.DATABASENAME,
                     PlatformUtils.determineDatabaseType(null, "jdbc:jtds:sqlserver://localhost:8080/test"));
    }

    /*
     * Tests the determination of the MySql platform via its JDBC drivers.
     */
    public void testMySqlDriver()
    {
        assertEquals(MySqlPlatform.DATABASENAME,
                     PlatformUtils.determineDatabaseType("com.mysql.jdbc.Driver", null));
        assertEquals(MySqlPlatform.DATABASENAME,
                     PlatformUtils.determineDatabaseType("org.gjt.mm.mysql.Driver", null));
    }

    /*
     * Tests the determination of the MySql platform via JDBC connection urls.
     */
    public void testMySqlUrl()
    {
        assertEquals(MySqlPlatform.DATABASENAME,
                     PlatformUtils.determineDatabaseType(null, "jdbc:mysql://localhost:1234/test"));
    }

    /*
     * Tests the determination of the Oracle 8 platform via its JDBC drivers.
     */
    public void testOracleDriver()
    {
        assertEquals(Oracle8Platform.DATABASENAME,
                     PlatformUtils.determineDatabaseType("oracle.jdbc.driver.OracleDriver", null));
        assertEquals(Oracle8Platform.DATABASENAME,
                     PlatformUtils.determineDatabaseType("oracle.jdbc.dnlddriver.OracleDriver", null));
        // DataDirect Connect
        assertEquals(Oracle8Platform.DATABASENAME,
                     PlatformUtils.determineDatabaseType("com.ddtek.jdbc.oracle.OracleDriver", null));
        // i-net
        assertEquals(Oracle8Platform.DATABASENAME,
                     PlatformUtils.determineDatabaseType("com.inet.ora.OraDriver", null));
    }

    /*
     * Tests the determination of the Oracle 8 platform via JDBC connection urls.
     */
    public void testOracleUrl()
    {
        assertEquals(Oracle8Platform.DATABASENAME,
                     PlatformUtils.determineDatabaseType(null, "jdbc:oracle:thin:@myhost:1521:orcl"));
        assertEquals(Oracle8Platform.DATABASENAME,
                     PlatformUtils.determineDatabaseType(null, "jdbc:oracle:oci8:@(description=(address=(host=myhost)(protocol=tcp)(port=1521))(connect_data=(sid=orcl)))"));
        assertEquals(Oracle8Platform.DATABASENAME,
                     PlatformUtils.determineDatabaseType(null, "jdbc:oracle:dnldthin:@myhost:1521:orcl"));
        assertEquals(Oracle8Platform.DATABASENAME,
                     PlatformUtils.determineDatabaseType(null, "jdbc:oracle:dnldthin:@myhost:1521:orcl"));
        // DataDirect Connect
        assertEquals(Oracle8Platform.DATABASENAME,
                     PlatformUtils.determineDatabaseType(null, "jdbc:datadirect:oracle://server3:1521;ServiceName=ORCL;User=test;Password=secret"));
        // i-net
        assertEquals(Oracle8Platform.DATABASENAME,
                     PlatformUtils.determineDatabaseType(null, "jdbc:inetora:www.inetsoftware.de:1521:orcl?traceLevel=2"));
    }

    /*
     * Tests the determination of the PostgreSql platform via its JDBC driver.
     */
    public void testPostgreSqlDriver()
    {
        assertEquals(PostgreSqlPlatform.DATABASENAME,
                     PlatformUtils.determineDatabaseType("org.postgresql.Driver", null));
    }

    /*
     * Tests the determination of the PostgreSql platform via JDBC connection urls.
     */
    public void testPostgreSqlUrl()
    {
        assertEquals(PostgreSqlPlatform.DATABASENAME,
                     PlatformUtils.determineDatabaseType(null, "jdbc:postgresql://localhost:1234/test"));
        assertEquals(PostgreSqlPlatform.DATABASENAME,
                     PlatformUtils.determineDatabaseType(null, "jdbc:postgresql://[::1]:5740/accounting"));
    }

    /*
     * Tests the determination of the Sybase platform via its JDBC drivers.
     */
    public void testSybaseDriver()
    {
        assertEquals(SybasePlatform.DATABASENAME,
                     PlatformUtils.determineDatabaseType("com.sybase.jdbc.SybDriver", null));
        assertEquals(SybasePlatform.DATABASENAME,
                     PlatformUtils.determineDatabaseType("com.sybase.jdbc2.jdbc.SybDriver", null));
        // DataDirect Connect
        assertEquals(SybasePlatform.DATABASENAME,
                     PlatformUtils.determineDatabaseType("com.ddtek.jdbc.sybase.SybaseDriver", null));
        // i-net
        assertEquals(SybasePlatform.DATABASENAME,
                     PlatformUtils.determineDatabaseType("com.inet.syb.SybDriver", null));
    }

    /*
     * Tests the determination of the Sybase platform via JDBC connection urls.
     */
    public void testSybaseUrl()
    {
        assertEquals(SybasePlatform.DATABASENAME,
                     PlatformUtils.determineDatabaseType(null, "jdbc:sybase:Tds:xyz:3767orjdbc:sybase:Tds:130.214.90.27:3767"));
        // DataDirect Connect
        assertEquals(SybasePlatform.DATABASENAME,
                     PlatformUtils.determineDatabaseType(null, "jdbc:datadirect:sybase://server2:5000;User=test;Password=secret"));
        // i-net
        assertEquals(SybasePlatform.DATABASENAME,
                     PlatformUtils.determineDatabaseType(null, "jdbc:inetsyb:www.inetsoftware.de:3333"));
        assertEquals(SybasePlatform.DATABASENAME,
                     PlatformUtils.determineDatabaseType(null, "jdbc:inetpool:inetsyb:www.inetsoftware.de:3333"));
        assertEquals(SybasePlatform.DATABASENAME,
                     PlatformUtils.determineDatabaseType(null, "jdbc:inetpool:jdbc:inetsyb:www.inetsoftware.de:3333"));
        // jTDS
        assertEquals(SybasePlatform.DATABASENAME,
                     PlatformUtils.determineDatabaseType(null, "jdbc:jtds:sybase://localhost:8080/test"));
    }
}
