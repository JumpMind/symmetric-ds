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
package org.jumpmind.symmetric.io.data.writer;

import java.util.List;

import org.jumpmind.db.model.Table;
import org.jumpmind.symmetric.io.data.CsvData;
import org.jumpmind.symmetric.io.data.DataEventType;
import org.jumpmind.symmetric.io.data.transform.TransformedData;

public class DefaultTransformWriterConflictResolver extends DefaultDatabaseWriterConflictResolver {

    protected TransformWriter transformWriter;

    public DefaultTransformWriterConflictResolver(TransformWriter transformWriter) {
        this.transformWriter = transformWriter;
    }

    @Override
    protected void performFallbackToInsert(DatabaseWriter writer, CsvData data, Conflict conflict, boolean retransform) {
        TransformedData transformedData = data.getAttribute(TransformedData.class.getName());
        if (transformedData != null && retransform) {
            List<TransformedData> newlyTransformedDatas = transformWriter.transform(
                    DataEventType.INSERT, writer.getContext(), transformedData.getTransformation(),
                    transformedData.getSourceKeyValues(), transformedData.getOldSourceValues(),
                    transformedData.getSourceValues());
            for (TransformedData newlyTransformedData : newlyTransformedDatas) {
                if (newlyTransformedData.hasSameKeyValues(transformedData.getKeyValues())
                        || newlyTransformedData.isGeneratedIdentityNeeded()) {
                    Table table = newlyTransformedData.buildTargetTable();
                    CsvData newData = newlyTransformedData.buildTargetCsvData();
                    String quote = writer.getPlatform().getDatabaseInfo().getDelimiterToken();
                    if (newlyTransformedData.isGeneratedIdentityNeeded()) {
                        if (log.isDebugEnabled()) {
                            log.debug("Enabling generation of identity for {}",
                                    newlyTransformedData.getTableName());
                        }
                        writer.getTransaction().allowInsertIntoAutoIncrementColumns(false, table, quote);
                    } else if (table.hasAutoIncrementColumn()) {
                        writer.getTransaction().allowInsertIntoAutoIncrementColumns(true, table, quote);
                    }

                    writer.start(table);
                    writer.write(newData);
                    writer.end(table);
                }
            }
        } else {
            super.performFallbackToInsert(writer, data, conflict, retransform);
        }
    }

    @Override
    protected void performFallbackToUpdate(DatabaseWriter writer, CsvData data, Conflict conflict, boolean retransform) {
        TransformedData transformedData = data.getAttribute(TransformedData.class.getName());
        if (transformedData != null && retransform) {
            List<TransformedData> newlyTransformedDatas = transformWriter.transform(
                    DataEventType.UPDATE, writer.getContext(), transformedData.getTransformation(),
                    transformedData.getSourceKeyValues(), transformedData.getOldSourceValues(),
                    transformedData.getSourceValues());
            for (TransformedData newlyTransformedData : newlyTransformedDatas) {
                if (newlyTransformedData.hasSameKeyValues(transformedData.getKeyValues())) {
                    Table table = newlyTransformedData.buildTargetTable();
                    writer.start(table);
                    writer.write(newlyTransformedData.buildTargetCsvData());
                    writer.end(table);
                }
            }
        } else {
            super.performFallbackToUpdate(writer, data, conflict, retransform);
        }
    }

}
