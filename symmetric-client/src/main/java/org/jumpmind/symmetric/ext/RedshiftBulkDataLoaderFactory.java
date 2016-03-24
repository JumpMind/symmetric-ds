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

import java.lang.reflect.Constructor;
import java.util.List;

import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.extension.IBuiltInExtensionPoint;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.io.data.IDataWriter;
import org.jumpmind.symmetric.io.data.writer.Conflict;
import org.jumpmind.symmetric.io.data.writer.IDatabaseWriterErrorHandler;
import org.jumpmind.symmetric.io.data.writer.IDatabaseWriterFilter;
import org.jumpmind.symmetric.io.data.writer.ResolvedData;
import org.jumpmind.symmetric.io.data.writer.TransformWriter;
import org.jumpmind.symmetric.io.stage.IStagingManager;
import org.jumpmind.symmetric.load.IDataLoaderFactory;
import org.jumpmind.symmetric.service.IParameterService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RedshiftBulkDataLoaderFactory implements IDataLoaderFactory, ISymmetricEngineAware, IBuiltInExtensionPoint {

    protected final Logger log = LoggerFactory.getLogger(getClass());
    
    private ISymmetricEngine engine;

    public RedshiftBulkDataLoaderFactory() {
    }

    public String getTypeName() {
        return "redshift_bulk";
    }

    public IDataWriter getDataWriter(String sourceNodeId, ISymmetricDialect symmetricDialect, TransformWriter transformWriter,
            List<IDatabaseWriterFilter> filters, List<IDatabaseWriterErrorHandler> errorHandlers,
            List<? extends Conflict> conflictSettings, List<ResolvedData> resolvedData) {

        IParameterService param = engine.getParameterService();
        int maxRowsBeforeFlush = param.getInt("redshift.bulk.load.max.rows.before.flush", 100000);
        long maxBytesBeforeFlush = param.getLong("redshift.bulk.load.max.bytes.before.flush", 1000000000);
        String bucket = param.getString("redshift.bulk.load.s3.bucket");
        String accessKey = param.getString("redshift.bulk.load.s3.access.key");
        String secretKey = param.getString("redshift.bulk.load.s3.secret.key");
        String appendToCopyCommand = param.getString("redshift.append.to.copy.command");
        String s3Endpoint = param.getString("redshift.bulk.load.s3.endpoint");

        try {
            Class<?> dbWriterClass = Class.forName("org.jumpmind.symmetric.io.RedshiftBulkDatabaseWriter");
            Constructor<?> dbWriterConstructor = dbWriterClass.getConstructor(new Class<?>[] {
                    IDatabasePlatform.class, IStagingManager.class, List.class,
                    List.class, Integer.TYPE, Long.TYPE, String.class,
                    String.class, String.class, String.class, String.class });
            return (IDataWriter) dbWriterConstructor.newInstance(
                    symmetricDialect.getPlatform(), engine.getStagingManager(), filters, errorHandlers,
                    maxRowsBeforeFlush, maxBytesBeforeFlush, bucket, accessKey, secretKey, appendToCopyCommand, s3Endpoint);

        } catch (Exception e) {
            log.warn("Failed to create the redshift database writer.  Check to see if all of the required jars have been added");
            if (e instanceof RuntimeException) {
                throw (RuntimeException)e;
            } else {
                throw new RuntimeException(e);
            }
        }
    }

    public void setSymmetricEngine(ISymmetricEngine engine) {
        this.engine = engine;
    }

    public boolean isPlatformSupported(IDatabasePlatform platform) {
        return true;
    }

}
