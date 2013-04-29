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
package org.jumpmind.symmetric;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.dbcp.BasicDataSource;
import org.h2.tools.Shell;

public class DbSqlCommand extends AbstractCommandLauncher {
    
    public DbSqlCommand() {
        super("dbsql", "", "DbSqlShell.Option.");
    }
    
    public static void main(String[] args) {
        new DbSqlCommand().execute(args);
    }    

    @Override
    protected boolean printHelpIfNoOptionsAreProvided() {
        return false;
    }

    @Override
    protected boolean requiresPropertiesFile() {
        return true;
    }

    @Override
    protected boolean executeWithOptions(CommandLine line) throws Exception {
        BasicDataSource basicDataSource = getDatabasePlatform(false).getDataSource();
        String url = basicDataSource.getUrl();
        String user = basicDataSource.getUsername();
        String password = basicDataSource.getPassword();
        String driver = basicDataSource.getDriverClassName();
        Shell shell = new Shell();
        shell.runTool("-url", url, "-user", user, "-password", password, "-driver", driver);
        return true;
    }


}
