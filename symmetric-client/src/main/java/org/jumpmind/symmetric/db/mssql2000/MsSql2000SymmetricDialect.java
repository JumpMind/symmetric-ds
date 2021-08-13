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
package org.jumpmind.symmetric.db.mssql2000;

import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.platform.PermissionType;
import org.jumpmind.db.util.BinaryEncoding;
import org.jumpmind.symmetric.db.mssql.MsSqlSymmetricDialect;
import org.jumpmind.symmetric.service.IParameterService;

public class MsSql2000SymmetricDialect extends MsSqlSymmetricDialect {
    public MsSql2000SymmetricDialect(IParameterService parameterService, IDatabasePlatform platform) {
        super(parameterService, platform);
        this.triggerTemplate = new MsSql2000TriggerTemplate(this);
    }

    @Override
    protected boolean alterLockEscalation() {
        return false;
    }

    @Override
    public void createRequiredDatabaseObjects() {
        String encode = this.parameterService.getTablePrefix() + "_" + "base64_encode";
        if (!installed(SQL_FUNCTION_INSTALLED, encode)) {
            String sql = "  create function dbo.$(functionName) (\n" +
                    "     @binvalue varbinary(8000)) returns varchar(8000)\n" +
                    "   as \n" +
                    "   begin\n" +
                    "   declare @charvalue varchar(8000)\n" +
                    "   declare @i int\n" +
                    "   declare @length int\n" +
                    "   declare @hexstring char(16)\n" +
                    "\n" +
                    "   select @charvalue = ''\n" +
                    "   select @i = 1\n" +
                    "   select @length = datalength(@binvalue)\n" +
                    "   select @hexstring = '0123456789abcdef'\n" +
                    "\n" +
                    "   while (@i <= @length)\n" +
                    "   begin\n" +
                    "\n" +
                    "     declare @tempint int\n" +
                    "     declare @firstint int\n" +
                    "     declare @secondint int\n" +
                    "\n" +
                    "     select @tempint = convert(int, substring(@binvalue,@i,1))\n" +
                    "     select @firstint = floor(@tempint/16)\n" +
                    "     select @secondint = @tempint - (@firstint*16)\n" +
                    "\n" +
                    "     select @charvalue = @charvalue +\n" +
                    "       substring(@hexstring, @firstint+1, 1) +\n" +
                    "       substring(@hexstring, @secondint+1, 1)\n" +
                    "\n" +
                    "     select @i = @i + 1\n" +
                    "   end\n" +
                    "    return @charvalue\n" +
                    "   end";
            install(sql, encode);
        }
        String triggersDisabled = this.parameterService.getTablePrefix() + "_" + "triggers_disabled";
        if (!installed(SQL_FUNCTION_INSTALLED, triggersDisabled)) {
            String sql = "create function dbo.$(functionName)() returns smallint                                                                                                                                                 \n"
                    +
                    "   begin                                                                                                          \n" +
                    "     declare @disabled varchar(1);\n" +
                    "     declare @context_info varbinary(128); \n" +
                    "     SELECT @Context_Info = CONTEXT_INFO\n" +
                    "        FROM master.dbo.SYSPROCESSES \n" +
                    "        WHERE SPID = @@SPID                                                                                                                                   \n"
                    +
                    "     set @disabled = coalesce(replace(substring(cast(@context_info as varchar), 1, 1), 0x0, ''), '');                                                                     \n"
                    +
                    "     if @disabled is null or @disabled != '1'                                                                                                                              \n"
                    +
                    "       return 0;                                                                                                                                                           \n"
                    +
                    "     return 1;                                                                                                                                                             \n"
                    +
                    "   end  ";
            install(sql, triggersDisabled);
        }
        String nodeDisabled = this.parameterService.getTablePrefix() + "_" + "node_disabled";
        if (!installed(SQL_FUNCTION_INSTALLED, nodeDisabled)) {
            String sql = "create function dbo.$(functionName)() returns varchar(50) \n" +
                    "  begin \n" +
                    "    declare @node varchar(50);\n" +
                    "    declare @context_info varbinary(128);\n" +
                    "    SELECT @context_info = CONTEXT_INFO\n" +
                    "        FROM master.dbo.SYSPROCESSES \n" +
                    "        WHERE SPID = @@SPID \n " +
                    "    SELECT @node = coalesce(replace(substring(cast(@context_info as varchar) collate SQL_Latin1_General_CP1_CI_AS, 2, 50), 0x0, ''), ''); \n "
                    +
                    "    return @node;                                                                                                                                                        "
                    +
                    "  end                                                                                                                                                                    ";
            install(sql, nodeDisabled);
        }
    }

    @Override
    public BinaryEncoding getBinaryEncoding() {
        return BinaryEncoding.HEX;
    }

    @Override
    public PermissionType[] getSymTablePermissions() {
        PermissionType[] permissions = { PermissionType.CREATE_TABLE, PermissionType.DROP_TABLE, PermissionType.CREATE_TRIGGER, PermissionType.DROP_TRIGGER,
                PermissionType.CREATE_FUNCTION };
        return permissions;
    }
}
