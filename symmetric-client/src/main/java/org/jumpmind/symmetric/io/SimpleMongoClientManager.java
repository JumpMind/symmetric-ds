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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.jumpmind.symmetric.service.IParameterService;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoDatabase;

public class SimpleMongoClientManager implements IMongoClientManager {

    protected IParameterService parameterService;

    protected String name = "mongodb";

    /*
     * This is static because the MongoClient is thread safe and wraps a pool of
     * connections
     */
    protected final static Map<String, MongoClient> clients = new HashMap<String, MongoClient>();

    protected MongoDatabase currentDB;

    public SimpleMongoClientManager(IParameterService parameterService, String name) {
        this.name = name;
        this.parameterService = parameterService;
    }

    @Override
    public synchronized  MongoClient getClient(String databaseName) {
        MongoClient client = clients.get(name);
        if (client == null) {
            int port = 27017;
            String host = "localhost";
            if (parameterService != null) {
                port = parameterService.getInt(name + MongoConstants.PORT, port);
                host = parameterService.getString(name + MongoConstants.HOST, host);
            }
            String dbUrl = "mongodb://" + host + ":" + port;
            String username = null;
            char[] password = null;
            if (parameterService != null) {
                dbUrl  = parameterService.getString(name + MongoConstants.URL, dbUrl);
                username = parameterService.getString(this.name + MongoConstants.USERNAME, username);
                String passwordString = parameterService.getString(this.name + MongoConstants.PASSWORD,
                        null);
                if (passwordString != null) {
                    password = passwordString.toCharArray();
                }
            }
            MongoCredential credential = null;
            credential = MongoCredential.createCredential(username, databaseName, password);
            client = new MongoClient(Arrays.asList(new ServerAddress(host, port)),
                    credential, new MongoClientOptions.Builder().build());
            clients.put(name, client);
        }
        return client;
    }

    @Override
    public synchronized MongoDatabase getDB(String databaseName) {
        if (currentDB == null || !currentDB.getName().equals(databaseName)) {
            currentDB = getClient(databaseName).getDatabase(databaseName);
            /**
             * TODO make this a property
             */
            currentDB.withWriteConcern(WriteConcern.ACKNOWLEDGED);
        }
        return currentDB;
    }

    @Override
    public String getName() {
        return name;
    }

}
