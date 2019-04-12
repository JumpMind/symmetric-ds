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
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.io.RedshiftBulkDatabaseWriter;
import org.jumpmind.symmetric.io.data.IDataWriter;
import org.jumpmind.symmetric.io.data.writer.Conflict;
import org.jumpmind.symmetric.io.data.writer.IDatabaseWriterErrorHandler;
import org.jumpmind.symmetric.io.data.writer.IDatabaseWriterFilter;
import org.jumpmind.symmetric.io.data.writer.ResolvedData;
import org.jumpmind.symmetric.io.data.writer.TransformWriter;
import org.jumpmind.symmetric.io.stage.IStagingManager;
import org.jumpmind.symmetric.load.AbstractDataLoaderFactory;
import org.jumpmind.symmetric.load.IDataLoaderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RedshiftBulkDataLoaderFactory extends AbstractDataLoaderFactory implements IDataLoaderFactory {

    protected final Logger log = LoggerFactory.getLogger(getClass());
    
    private IStagingManager stagingManager;

    public RedshiftBulkDataLoaderFactory(ISymmetricEngine engine) {
        this.parameterService = engine.getParameterService();
        this.stagingManager = engine.getStagingManager();
    }

    public String getTypeName() {
        return "redshift_bulk";
    }

    public IDataWriter getDataWriter(String sourceNodeId, ISymmetricDialect symmetricDialect,
    			TransformWriter transformWriter,
            List<IDatabaseWriterFilter> filters, List<IDatabaseWriterErrorHandler> errorHandlers,
            List<? extends Conflict> conflictSettings, List<ResolvedData> resolvedData) {

        return new RedshiftBulkDatabaseWriter(symmetricDialect.getPlatform(), symmetricDialect.getTargetPlatform(), 
                symmetricDialect.getTablePrefix(), stagingManager, filters, errorHandlers, parameterService, buildParameterDatabaseWritterSettings());
        
    }

    public boolean isPlatformSupported(IDatabasePlatform platform) {
        return DatabaseNamesConstants.REDSHIFT.equals(platform.getName());
    }

}
