package org.jumpmind.symmetric.io.data.writer;

import com.mongodb.DB;
import com.mongodb.MongoClient;

public interface IMongoClientManager {

    public MongoClient get();
    
    public DB getDB(String name);
    
    public String getName();
    
}
