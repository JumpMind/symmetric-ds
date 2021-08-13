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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jumpmind.db.platform.DatabaseNamesConstants;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.extension.IBuiltInExtensionPoint;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.io.JdbcBatchBulkDatabaseWriter;
import org.jumpmind.symmetric.io.data.IDataWriter;
import org.jumpmind.symmetric.io.data.writer.Conflict;
import org.jumpmind.symmetric.io.data.writer.IDatabaseWriterErrorHandler;
import org.jumpmind.symmetric.io.data.writer.IDatabaseWriterFilter;
import org.jumpmind.symmetric.io.data.writer.ResolvedData;
import org.jumpmind.symmetric.io.data.writer.TransformWriter;
import org.jumpmind.symmetric.load.AbstractDataLoaderFactory;
import org.jumpmind.symmetric.load.IDataLoaderFactory;

public class BulkDataLoaderFactory extends AbstractDataLoaderFactory implements IDataLoaderFactory, ISymmetricEngineAware, IBuiltInExtensionPoint {
    ISymmetricEngine engine;
    Map<String, IDataLoaderFactory> dataLoaderFactories = new HashMap<String, IDataLoaderFactory>();

    @Override
    public String getTypeName() {
        return "bulk";
    }

    @Override
    public IDataWriter getDataWriter(String sourceNodeId, ISymmetricDialect symmetricDialect, TransformWriter transformWriter,
            List<IDatabaseWriterFilter> filters, List<IDatabaseWriterErrorHandler> errorHandlers,
            List<? extends Conflict> conflictSettings, List<ResolvedData> resolvedData) {
        for (IDataLoaderFactory factory : engine.getExtensionService().getExtensionPointList(IDataLoaderFactory.class)) {
            dataLoaderFactories.put(factory.getTypeName(), factory);
        }
        IDatabasePlatform platform = engine.getTargetDialect().getPlatform();
        String platformName = platform.getName();
        if (engine.getParameterService().is(ParameterConstants.JDBC_EXECUTE_BULK_BATCH_OVERRIDE, false)) {
            return new JdbcBatchBulkDatabaseWriter(symmetricDialect.getPlatform(), platform,
                    symmetricDialect.getTablePrefix(), buildParameterDatabaseWritterSettings());
        } else if (DatabaseNamesConstants.MYSQL.equals(platformName)) {
            return new MySqlBulkDataLoaderFactory(engine).getDataWriter(sourceNodeId, symmetricDialect, transformWriter,
                    filters, errorHandlers, conflictSettings, resolvedData);
        } else if (DatabaseNamesConstants.MSSQL2000.equals(platformName)
                || DatabaseNamesConstants.MSSQL2005.equals(platformName)
                || DatabaseNamesConstants.MSSQL2008.equals(platformName)
                || DatabaseNamesConstants.MSSQL2016.equals(platformName)) {
            return new MsSqlBulkDataLoaderFactory(engine).getDataWriter(sourceNodeId, symmetricDialect, transformWriter,
                    filters, errorHandlers, conflictSettings, resolvedData);
        } else if (DatabaseNamesConstants.ORACLE.equals(platformName) || DatabaseNamesConstants.ORACLE122.equals(platformName)) {
            return new OracleBulkDataLoaderFactory(engine).getDataWriter(sourceNodeId, symmetricDialect, transformWriter,
                    filters, errorHandlers, conflictSettings, resolvedData);
        } else if (DatabaseNamesConstants.TIBERO.equals(platformName)) {
            return new TiberoBulkDataLoaderFactory(engine).getDataWriter(sourceNodeId, symmetricDialect, transformWriter,
                    filters, errorHandlers, conflictSettings, resolvedData);
        } else if (DatabaseNamesConstants.POSTGRESQL.equals(platformName)
                || DatabaseNamesConstants.POSTGRESQL95.equals(platformName)
                || DatabaseNamesConstants.GREENPLUM.equals(platformName)) {
            return new PostgresBulkDataLoaderFactory(engine).getDataWriter(sourceNodeId, symmetricDialect, transformWriter,
                    filters, errorHandlers, conflictSettings, resolvedData);
        } else if (DatabaseNamesConstants.REDSHIFT.equals(platformName)) {
            return new RedshiftBulkDataLoaderFactory(engine).getDataWriter(sourceNodeId, symmetricDialect, transformWriter,
                    filters, errorHandlers, conflictSettings, resolvedData);
        } else if (platformName != null && platformName.startsWith(DatabaseNamesConstants.TERADATA)) {
            return new TeradataBulkDataLoaderFactory(engine).getDataWriter(sourceNodeId, symmetricDialect, transformWriter,
                    filters, errorHandlers, conflictSettings, resolvedData);
        } else if (platformName != null && platformName.startsWith(DatabaseNamesConstants.SNOWFLAKE)) {
            return new SnowflakeBulkDataLoaderFactory(engine).getDataWriter(sourceNodeId, symmetricDialect, transformWriter,
                    filters, errorHandlers, conflictSettings, resolvedData);
        } else if (DatabaseNamesConstants.BIGQUERY.equals(engine.getSymmetricDialect().getTargetPlatform().getName())) {
            return new BigQueryDataLoaderFactory(engine).getDataWriter(sourceNodeId, symmetricDialect, transformWriter,
                    filters, errorHandlers, conflictSettings, resolvedData);
        } else {
            return new JdbcBatchBulkDatabaseWriter(symmetricDialect.getPlatform(), platform,
                    symmetricDialect.getTablePrefix(), buildParameterDatabaseWritterSettings());
        }
    }

    @Override
    public boolean isPlatformSupported(IDatabasePlatform platform) {
        return true;
    }

    @Override
    public void setSymmetricEngine(ISymmetricEngine engine) {
        this.engine = engine;
        this.parameterService = engine.getParameterService();
    }
}
