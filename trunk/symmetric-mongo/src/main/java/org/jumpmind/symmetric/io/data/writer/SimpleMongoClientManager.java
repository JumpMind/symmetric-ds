package org.jumpmind.symmetric.io.data.writer;

import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import org.jumpmind.symmetric.SymmetricException;
import org.jumpmind.symmetric.service.IParameterService;

import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.WriteConcern;

public class SimpleMongoClientManager implements IMongoClientManager {

    protected IParameterService parameterService;

    protected String name = "mongodb";

    /*
     * This is static because the MongoClient is thread safe and wraps a pool of
     * connections
     */
    protected static Map<String, MongoClient> clients = new HashMap<String, MongoClient>();

    protected DB currentDB;

    public SimpleMongoClientManager(IParameterService parameterService, String name) {
        this.name = name;
        this.parameterService = parameterService;
    }

    @Override
    public MongoClient get() {
        MongoClient client = clients.get(name);
        if (client == null) {
            synchronized (clients) {
                if (client == null) {
                    int port = 27017;
                    String host = "localhost";
                    if (parameterService != null) {
                        port = parameterService.getInt(name + MongoConstants.PORT, port);
                        host = parameterService.getString(name + MongoConstants.HOST, host);
                    }

                    try {
                        client = new MongoClient(host, port);
                        clients.put(name, client);
                    } catch (UnknownHostException e) {
                        throw new SymmetricException(e);
                    }
                }
            }
        }
        return client;
    }

    @Override
    public DB getDB(String name) {
        if (currentDB == null || !currentDB.getName().equals(name)) {
            currentDB = get().getDB(name);
            /**
             * TODO make this a property
             */
            currentDB.setWriteConcern(WriteConcern.ACKNOWLEDGED);
            String username = null;
            char[] password = null;
            if (parameterService != null) {
                username = parameterService.getString(name + MongoConstants.USERNAME, username);
                String passwordString = parameterService.getString(name + MongoConstants.PASSWORD,
                        null);
                if (passwordString != null) {
                    password = passwordString.toCharArray();
                }
            }

            if (username != null && !currentDB.authenticate(username, password)) {
                throw new SymmetricException("Failed to authenticate with the mongo database: "
                        + name);
            }
        }
        return currentDB;
    }

    @Override
    public String getName() {
        return name;
    }

}
