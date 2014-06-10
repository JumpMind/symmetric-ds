package org.jumpmind.symmetric.io.data.writer;

import static org.apache.commons.lang.StringUtils.isBlank;
import static org.apache.commons.lang.StringUtils.isNotBlank;

import java.util.Map;
import java.util.Set;

import org.jumpmind.db.model.Table;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class SimpleDBObjectMapper implements IDBObjectMapper {

    String defaultDatabaseName = "default";

    public DBObject mapToDBObject(Table table, Map<String, String> newData,
            Map<String, String> oldData, Map<String, String> pkData, boolean mapKeyOnly) {
        if (mapKeyOnly) {
            return buildWithKey(table, newData, oldData, pkData);
        } else {
            return buildWithKeyAndData(table, newData, oldData, pkData);
        }
    }

    public String mapToCollection(Table table) {
        return table.getName();
    }

    public String mapToDatabase(Table table) {
        String mongoDatabaseName = table.getCatalog();
        if (isNotBlank(mongoDatabaseName) && isNotBlank(table.getSchema())) {
            mongoDatabaseName += ".";
        } else {
            mongoDatabaseName = "";
        }

        if (isNotBlank(table.getSchema())) {
            mongoDatabaseName += table.getSchema();
        }

        if (isBlank(mongoDatabaseName)) {
            mongoDatabaseName = defaultDatabaseName;
        }

        return mongoDatabaseName;
    }

    protected BasicDBObject buildWithKey(Table table, Map<String, String> newData,
            Map<String, String> oldData, Map<String, String> pkData) {
        if (oldData == null || oldData.size() == 0) {
            oldData = pkData;
        }
        if (oldData == null || oldData.size() == 0) {
            oldData = newData;
        }

        String[] keyNames = table.getPrimaryKeyColumnNames();

        BasicDBObject object = new BasicDBObject();

        // TODO support property to just let mongodb create ids?
        if (keyNames != null && keyNames.length > 0) {
            if (keyNames.length == 1) {
                object.put("_id", oldData.get(keyNames[0]));
            } else {
                BasicDBObject key = new BasicDBObject();
                for (String keyName : keyNames) {
                    key.put(keyName, oldData.get(keyName));
                }
                object.put("_id", key);
            }
        }

        return object;
    }

    protected BasicDBObject buildWithKeyAndData(Table table, Map<String, String> newData,
            Map<String, String> oldData, Map<String, String> pkData) {
        BasicDBObject object = buildWithKey(table, newData, oldData, pkData);

        Set<String> newDataKeys = newData.keySet();
        for (String newDataKey : newDataKeys) {
            object.put(newDataKey, newData.get(newDataKey));
        }

        return object;
    }

    public void setDefaultDatabaseName(String defaultDatabaseName) {
        this.defaultDatabaseName = defaultDatabaseName;
    }

}
