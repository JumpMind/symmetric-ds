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
package org.jumpmind.symmetric.common;

/**
 * These are properties that are server wide. They can be accessed via the parameter service or via System properties.
 */
public class ServerConstants {
    public final static String HOST_BIND_NAME = "host.bind.name";
    public final static String HTTP_ENABLE = "http.enable";
    public final static String HTTP_PORT = "http.port";
    public final static String HTTPS_ENABLE = "https.enable";
    public final static String HTTPS_PORT = "https.port";
    public final static String HTTPS2_ENABLE = "https2.enable";
    public final static String HTTPS_VERIFIED_SERVERS = "https.verified.server.names";
    public final static String HTTPS_ALLOW_SELF_SIGNED_CERTS = "https.allow.self.signed.certs";
    public final static String HTTPS_NEED_CLIENT_AUTH = "https.need.client.auth";
    public final static String HTTPS_WANT_CLIENT_AUTH = "https.want.client.auth";
    public static final String SERVER_ALLOW_DIR_LISTING = "server.allow.dir.list";
    public static final String SERVER_ALLOW_HTTP_METHODS = "server.allow.http.methods";
    public static final String SERVER_DISALLOW_HTTP_METHODS = "server.disallow.http.methods";
    public final static String SERVER_HTTP_COOKIES_ENABLED = "server.http.cookies.enabled";
    public final static String STREAM_TO_FILE_ENCRYPT_ENABLED = "stream.to.file.encrypt.enabled";
    public final static String STREAM_TO_FILE_COMPRESSION_ENABLED = "stream.to.file.compression.enabled";
    public final static String STREAM_TO_FILE_COMPRESSION_LEVEL = "stream.to.file.compression.level";
    public final static String SERVER_ENGINE_URI_INTERCEPTORS = "server.engine.uri.interceptors";
    public final static String SERVER_ACCESS_LOG_ENABLED = "server.access.log.enabled";
    public final static String SERVER_ACCESS_LOG_FILE = "server.access.log.file";
}