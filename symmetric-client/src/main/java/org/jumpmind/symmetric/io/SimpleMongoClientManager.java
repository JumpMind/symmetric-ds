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
    protected final static Map<String, MongoClient> clients = new HashMap<String, MongoClient>();

    protected DB currentDB;

    public SimpleMongoClientManager(IParameterService parameterService, String name) {
        this.name = name;
        this.parameterService = parameterService;
    }

    @Override
    public synchronized  MongoClient get() {
        MongoClient client = clients.get(name);
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
        return client;
    }

    @Override
    public synchronized DB getDB(String name) {
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
