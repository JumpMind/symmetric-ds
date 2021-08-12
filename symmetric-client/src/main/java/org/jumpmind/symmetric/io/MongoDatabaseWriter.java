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

import java.util.Map;

import org.bson.Document;
import org.jumpmind.symmetric.SymmetricException;
import org.jumpmind.symmetric.io.data.CsvData;
import org.jumpmind.symmetric.io.data.writer.AbstractDatabaseWriter;
import org.jumpmind.symmetric.io.data.writer.DataWriterStatisticConstants;
import org.jumpmind.symmetric.io.data.writer.DatabaseWriterSettings;
import org.jumpmind.symmetric.io.data.writer.IDatabaseWriterConflictResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;

/**
 * The default mapping is that a catalog or schema or catalog.schema is mapped to a Mongo database that is named the same as the catalog and/or schema and/or
 * pair combined.
 */
public class MongoDatabaseWriter extends AbstractDatabaseWriter {
    /*
     * TODO talk about initial load. if reload channel is set to mongodb then sym_node_security records would be written to mongo unless we filtered out sym_
     * records. sym_node_security would never get updated in the regular database if i turn off the use.reload property initial load works.
     * 
     * TODO It looks like mongodb handles strings, byte[] and date() objects how do we determine when to insert certain types because there is no schema in
     * mongo db. One idea I had was to cache create xml for tables somewhere in mongodb and use it to determine types from the source.
     * 
     * TODO support mapping foreign keys into references
     * 
     * TODO could add support for bulk inserts. the insert api can take an array of dbobjects
     * 
     * TODO property for write concern http://api.mongodb.org/java/current/com/mongodb /WriteConcern.html#ACKNOWLEDGED
     */
    private static final Logger log = LoggerFactory.getLogger(MongoDatabaseWriter.class);
    protected IMongoClientManager clientManager;
    protected IDBObjectMapper objectMapper;

    public MongoDatabaseWriter(IDBObjectMapper objectMapper, IMongoClientManager clientManager,
            IDatabaseWriterConflictResolver conflictResolver, DatabaseWriterSettings settings) {
        super(conflictResolver, settings);
        this.clientManager = clientManager;
        this.objectMapper = objectMapper;
    }

    @Override
    protected LoadStatus insert(CsvData data) {
        return upsert(data);
    }

    @Override
    protected LoadStatus update(CsvData data, boolean applyChangesOnly, boolean useConflictDetection) {
        return upsert(data);
    }

    @Override
    protected void logFailureDetails(Throwable e, CsvData data, boolean logLastDmlDetails) {
        /*
         * Stacktrace will be printed after the error continues to bubble up
         */
    }

    protected LoadStatus upsert(CsvData data) {
        statistics.get(batch).startTimer(DataWriterStatisticConstants.LOADMILLIS);
        try {
            MongoDatabase db = clientManager.getDB(objectMapper.mapToDatabase(this.targetTable));
            MongoCollection<Document> collection = db.getCollection(objectMapper
                    .mapToCollection(this.targetTable));
            String[] columnNames = sourceTable.getColumnNames();
            Map<String, String> newData = data
                    .toColumnNameValuePairs(columnNames, CsvData.ROW_DATA);
            Map<String, String> oldData = data
                    .toColumnNameValuePairs(columnNames, CsvData.OLD_DATA);
            Map<String, String> pkData = data.toKeyColumnValuePairs(this.sourceTable);
            Document query = objectMapper
                    .mapToDocument(sourceTable, newData, oldData, pkData, true);
            Document object = objectMapper.mapToDocument(sourceTable, newData, oldData, pkData,
                    false);
            UpdateResult results = collection.replaceOne(query, object, new ReplaceOptions().upsert(true));
            if (results.getModifiedCount() == 1 || results.getUpsertedId() != null) {
                return LoadStatus.SUCCESS;
            } else {
                throw new SymmetricException("Failed to write data: " + object);
            }
        } finally {
            statistics.get(batch).stopTimer(DataWriterStatisticConstants.LOADMILLIS);
        }
    }

    @Override
    protected LoadStatus delete(CsvData data, boolean useConflictDetection) {
        statistics.get(batch).startTimer(DataWriterStatisticConstants.LOADMILLIS);
        try {
            MongoDatabase db = clientManager.getDB(objectMapper.mapToDatabase(this.targetTable));
            MongoCollection<Document> collection = db.getCollection(objectMapper
                    .mapToCollection(this.targetTable));
            String[] columnNames = sourceTable.getColumnNames();
            Map<String, String> newData = data
                    .toColumnNameValuePairs(columnNames, CsvData.ROW_DATA);
            Map<String, String> oldData = data
                    .toColumnNameValuePairs(columnNames, CsvData.OLD_DATA);
            Map<String, String> pkData = data.toKeyColumnValuePairs(this.sourceTable);
            Document query = objectMapper
                    .mapToDocument(sourceTable, newData, oldData, pkData, true);
            DeleteResult results = collection.deleteOne(query);
            if (results.getDeletedCount() != 1) {
                log.warn("Attempted to remove a single object" + query
                        + ".  Instead removed: " + results.getDeletedCount());
            }
            return LoadStatus.SUCCESS;
        } finally {
            statistics.get(batch).stopTimer(DataWriterStatisticConstants.LOADMILLIS);
        }
    }

    @Override
    protected boolean create(CsvData data) {
        return true;
    }

    @Override
    protected boolean sql(CsvData data) {
        statistics.get(batch).startTimer(DataWriterStatisticConstants.LOADMILLIS);
        try {
            MongoDatabase db = clientManager.getDB(objectMapper.mapToDatabase(this.targetTable));
            String command = data.getParsedData(CsvData.ROW_DATA)[0];
            log.info("About to run command: {}", command);
            Document results = db.runCommand(new Document(command, 1));
            log.info("The results of the command were: {}", results);
        } finally {
            statistics.get(batch).stopTimer(DataWriterStatisticConstants.LOADMILLIS);
        }
        return true;
    }
}
