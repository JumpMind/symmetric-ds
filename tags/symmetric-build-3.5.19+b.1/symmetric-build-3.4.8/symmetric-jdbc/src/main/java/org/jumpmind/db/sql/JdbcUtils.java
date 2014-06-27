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
 * under the License. 
 */
package org.jumpmind.db.sql;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.support.nativejdbc.CommonsDbcpNativeJdbcExtractor;
import org.springframework.jdbc.support.nativejdbc.NativeJdbcExtractor;

abstract public class JdbcUtils {
    
    private static Logger log = LoggerFactory.getLogger(JdbcUtils.class);

    private JdbcUtils() {
    }    
    
    public static NativeJdbcExtractor getNativeJdbcExtractory () {
        try {
            return (NativeJdbcExtractor) Class
                    .forName(
                            System.getProperty("db.native.extractor",
                                    "org.springframework.jdbc.support.nativejdbc.CommonsDbcpNativeJdbcExtractor"))
                    .newInstance();
        } catch (Exception ex) {
            log.error("The native jdbc extractor has not been configured.  Defaulting to the common basic datasource extractor.", ex);
            return new CommonsDbcpNativeJdbcExtractor();
        }
    }

}
