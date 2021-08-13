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
package org.jumpmind.symmetric.io;

import java.io.File;

import org.apache.commons.lang3.StringUtils;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.symmetric.io.data.writer.DatabaseWriterSettings;
import org.jumpmind.symmetric.io.stage.IStagingManager;
import org.slf4j.LoggerFactory;

public class TiberoBulkDatabaseWriter extends OracleBulkDatabaseWriter {
    public TiberoBulkDatabaseWriter(IDatabasePlatform symmetricPlatform, IDatabasePlatform targetPlatform,
            IStagingManager stagingManager, String tablePrefix, String tbLoaderCommand, String tbLoaderOptions,
            String dbUser, String dbPassword, String dbUrl, String dbName, String sqlLoaderInfileCharset,
            String fieldTerminator, String lineTerminator,
            DatabaseWriterSettings settings, boolean delimitTokens) {
        super(symmetricPlatform, targetPlatform, stagingManager, tablePrefix, tbLoaderCommand, tbLoaderOptions,
                dbUser, dbPassword, dbUrl, dbName, sqlLoaderInfileCharset, fieldTerminator, lineTerminator, settings,
                delimitTokens);
        logger = LoggerFactory.getLogger(getClass());
        ezConnectString = StringUtils.defaultIfBlank(dbName, getDbConnectString(dbUrl));
        if (StringUtils.isBlank(sqlLoaderCommand)) {
            String tiberoHome = System.getenv("TB_HOME");
            if (StringUtils.isNotBlank(tiberoHome)) {
                sqlLoaderCommand = tiberoHome + File.separator + "client" + File.separator + "bin" + File.separator + "tbloader";
            } else {
                sqlLoaderCommand = "tbloader";
            }
        }
    }

    @Override
    protected String getInfileControl() {
        return "INFILE '" + dataResource.getFile().getName() + "'\n";
    }

    @Override
    protected String getLineTerminatedByControl() {
        return "LINES TERMINATED BY '" + lineTerminator + "'\n";
    }

    @Override
    protected String getLoaderName() {
        return "TBLoader";
    }

    protected final String getDbConnectString(String dbUrl) {
        String connectStr = "";
        int index = dbUrl.lastIndexOf(":");
        if (index != -1) {
            connectStr = "@" + dbUrl.substring(index + 1);
        }
        return connectStr;
    }
}
