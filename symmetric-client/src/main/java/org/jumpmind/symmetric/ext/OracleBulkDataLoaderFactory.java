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
package org.jumpmind.symmetric.ext;

import java.util.List;

import org.jumpmind.db.platform.DatabaseNamesConstants;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.util.BasicDataSourcePropertyConstants;
import org.jumpmind.security.SecurityConstants;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.io.OracleBulkDatabaseWriter;
import org.jumpmind.symmetric.io.data.IDataWriter;
import org.jumpmind.symmetric.io.data.writer.Conflict;
import org.jumpmind.symmetric.io.data.writer.IDatabaseWriterErrorHandler;
import org.jumpmind.symmetric.io.data.writer.IDatabaseWriterFilter;
import org.jumpmind.symmetric.io.data.writer.ResolvedData;
import org.jumpmind.symmetric.io.data.writer.TransformWriter;
import org.jumpmind.symmetric.load.DefaultDataLoaderFactory;
import org.jumpmind.symmetric.service.IParameterService;

public class OracleBulkDataLoaderFactory extends DefaultDataLoaderFactory {

    private ISymmetricEngine engine;

    public OracleBulkDataLoaderFactory(ISymmetricEngine engine) {
        this.engine = engine;
        this.parameterService = engine.getParameterService();
    }

    public String getTypeName() {
        return "oracle_bulk";
    }

    public IDataWriter getDataWriter(String sourceNodeId, ISymmetricDialect symmetricDialect,
    			TransformWriter transformWriter, List<IDatabaseWriterFilter> filters,
            List<IDatabaseWriterErrorHandler> errorHandlers,
            List<? extends Conflict> conflictSettings, List<ResolvedData> resolvedData) {
    	
    	IParameterService parmService = engine.getParameterService();
    	String dbUrl = parmService.getString(BasicDataSourcePropertyConstants.DB_POOL_URL);
        String dbUser = parmService.getString(BasicDataSourcePropertyConstants.DB_POOL_USER);
		if (dbUser != null && dbUser.startsWith(SecurityConstants.PREFIX_ENC)) {
			dbUser = engine.getSecurityService().decrypt(dbUser.substring(SecurityConstants.PREFIX_ENC.length()));
		}

		String dbPassword = parmService.getString(BasicDataSourcePropertyConstants.DB_POOL_PASSWORD);
		if (dbPassword != null && dbPassword.startsWith(SecurityConstants.PREFIX_ENC)) {
			dbPassword = engine.getSecurityService().decrypt(dbPassword.substring(SecurityConstants.PREFIX_ENC.length()));
		}

		String sqlLoaderCommand = parmService.getString(ParameterConstants.DBDIALECT_ORACLE_BULK_LOAD_SQLLDR_CMD);
		String sqlLoaderOptions = parmService.getString(ParameterConstants.DBDIALECT_ORACLE_BULK_LOAD_SQLLDR_OPTIONS);
        String ezConnectString = parmService.getString(ParameterConstants.DBDIALECT_ORACLE_BULK_LOAD_EZCONNECT);

        return new OracleBulkDatabaseWriter(symmetricDialect.getPlatform(), symmetricDialect.getTargetPlatform(),
        		engine.getStagingManager(), engine.getTablePrefix(), sqlLoaderCommand, sqlLoaderOptions,
        		dbUser, dbPassword, dbUrl, ezConnectString,
        		buildDatabaseWriterSettings(filters, errorHandlers, conflictSettings, resolvedData));
    }

    public boolean isPlatformSupported(IDatabasePlatform platform) {
        return DatabaseNamesConstants.ORACLE.equals(platform.getName());
    }

}
