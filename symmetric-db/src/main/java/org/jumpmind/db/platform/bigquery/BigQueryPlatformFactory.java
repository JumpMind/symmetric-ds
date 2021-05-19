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
package org.jumpmind.db.platform.bigquery;

import java.io.FileInputStream;

import org.jumpmind.db.sql.SqlTemplateSettings;

import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.http.HttpTransportOptions;

public class BigQueryPlatformFactory {

    public BigQueryPlatformFactory() {
    }

    public BigQueryPlatform createDatabasePlatform(SqlTemplateSettings settings, String projectId, String location, String filePath) {
        try {
            HttpTransportOptions transportOptions = BigQueryOptions.getDefaultHttpTransportOptions();
            transportOptions = transportOptions.toBuilder().setConnectTimeout(60000).setReadTimeout(60000).build();

            BigQuery bigquery = BigQueryOptions.newBuilder().setProjectId(projectId).setLocation(location)
                    .setCredentials(ServiceAccountCredentials.fromStream(new FileInputStream(filePath)))
                    .setTransportOptions(transportOptions).build().getService();
            
            return new BigQueryPlatform(settings, bigquery);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
