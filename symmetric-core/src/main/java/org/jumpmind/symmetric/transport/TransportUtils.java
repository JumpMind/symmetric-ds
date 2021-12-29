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
package org.jumpmind.symmetric.transport;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.web.WebConstants;
import org.jumpmind.util.AppUtils;

public class TransportUtils {
    public static BufferedReader toReader(InputStream is) {
        return new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8));
    }

    public static BufferedWriter toWriter(OutputStream os) {
        return new BufferedWriter(new OutputStreamWriter(os, StandardCharsets.UTF_8));
    }

    public static String toCSV(Map<?, ?> map) {
        if (map == null || map.isEmpty()) {
            return "";
        }
        StringBuilder buff = new StringBuilder();
        for (Object key : map.keySet()) {
            buff.append(key).append(":").append(map.get(key)).append(",");
        }
        buff.setLength(buff.length() - 1);
        return buff.toString();
    }

    public static Node convertPropertiesToNode(Map<String, String> prop) throws IOException {
        Node node = new Node();
        node.setNodeGroupId(prop.get(WebConstants.NODE_GROUP_ID));
        node.setExternalId(prop.get(WebConstants.EXTERNAL_ID));
        node.setSyncUrl(prop.get(WebConstants.SYNC_URL));
        node.setSchemaVersion(prop.get(WebConstants.SCHEMA_VERSION));
        node.setDatabaseType(prop.get(WebConstants.DATABASE_TYPE));
        node.setDatabaseVersion(prop.get(WebConstants.DATABASE_VERSION));
        node.setDatabaseName(prop.get(WebConstants.DATABASE_NAME));
        node.setSymmetricVersion(prop.get(WebConstants.SYMMETRIC_VERSION));
        node.setDeploymentType(prop.get(WebConstants.DEPLOYMENT_TYPE));
        return node;
    }

    public static Map<String, String> convertNodeToProperties(Node node, Map<String, String> prop) {
        if (prop == null) {
            prop = new HashMap<String, String>();
        }
        prop.put(WebConstants.NODE_GROUP_ID, node.getNodeGroupId());
        prop.put(WebConstants.EXTERNAL_ID, node.getExternalId());
        prop.put(WebConstants.SYNC_URL, node.getSyncUrl());
        prop.put(WebConstants.SCHEMA_VERSION, node.getSchemaVersion());
        prop.put(WebConstants.DATABASE_TYPE, node.getDatabaseType());
        prop.put(WebConstants.DATABASE_VERSION, node.getDatabaseVersion());
        prop.put(WebConstants.DATABASE_NAME, node.getDatabaseName());
        prop.put(WebConstants.SYMMETRIC_VERSION, node.getSymmetricVersion());
        prop.put(WebConstants.DEPLOYMENT_TYPE, node.getDeploymentType());
        prop.put(WebConstants.HOST_NAME, AppUtils.getHostName());
        prop.put(WebConstants.IP_ADDRESS, AppUtils.getIpAddress());
        return prop;
    }
}