/*
 * Licensed to JumpMind Inc under one or more contributor 
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding 
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU Lesser General Public License (the
 * "License"); you may not use this file except in compliance
 * with the License. 
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see           
 * <http://www.gnu.org/licenses/>.
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.  */

package org.jumpmind.symmetric.common;

public class SecurityConstants {

    public static final String PREFIX_ENC = "enc:";

    public static final String ALGORITHM = System.getProperty("sym.secret.key.defalt.algorithm","PBEWithMD5AndDES");

    public static final int ITERATION_COUNT = 3;

    public static final String CHARSET = "UTF8";

    public static final String KEYSTORE_PASSWORD = "changeit";
    
    public static final String KEYSTORE_TYPE = "JCEKS";

    public static final byte[] SALT = { (byte) 0x01, (byte) 0x03, (byte) 0x05, (byte) 0x07, (byte) 0xA2,
            (byte) 0xB4, (byte) 0xC6, (byte) 0xD8 };
    
    public static final String ALIAS_SYM_PRIVATE_KEY = "sym";
    
    public static final String ALIAS_SYM_SECRET_KEY = "sym.secret";
    
    public static final String EMBEDDED_WEBSERVER_DEFAULT_ROLE="symmetric";

}