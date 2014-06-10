package org.jumpmind.symmetric.io.data.writer;

import java.util.Map;

import org.jumpmind.db.model.Table;

import com.mongodb.DBObject;

public interface IDBObjectMapper {

    public DBObject mapToDBObject(Table table, Map<String, String> newData, Map<String, String> oldData,
            Map<String, String> pkData, boolean mapKeyOnly);

    public String mapToCollection(Table table);

    public String mapToDatabase(Table table);

}
