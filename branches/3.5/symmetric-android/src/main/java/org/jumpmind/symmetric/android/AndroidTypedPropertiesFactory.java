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
package org.jumpmind.symmetric.android;

import java.io.IOException;
import java.util.Properties;

import org.jumpmind.exception.IoException;
import org.jumpmind.properties.TypedProperties;
import org.jumpmind.symmetric.ITypedPropertiesFactory;
import org.jumpmind.symmetric.common.ParameterConstants;

public class AndroidTypedPropertiesFactory implements ITypedPropertiesFactory {

    private TypedProperties properties;

    public AndroidTypedPropertiesFactory(String syncUrl, String externalId, String nodeGroupId,
            Properties defaultProperties) {
        properties = new TypedProperties();
        try {
            properties.load(getClass().getResourceAsStream("/symmetric-default.properties"));
            // change the default settings to make sense for android
            properties.setProperty(ParameterConstants.STREAM_TO_FILE_ENABLED,
                    Boolean.FALSE.toString());
            properties.put(ParameterConstants.REGISTRATION_URL, syncUrl);
            properties.put(ParameterConstants.EXTERNAL_ID, externalId);
            properties.put(ParameterConstants.NODE_GROUP_ID, nodeGroupId);
            properties.put(ParameterConstants.START_STATISTIC_FLUSH_JOB, "false");
            properties.put(ParameterConstants.START_STAGE_MGMT_JOB, "false");
            properties.put(ParameterConstants.START_SYNCTRIGGERS_JOB, "false");
            properties.put(ParameterConstants.START_WATCHDOG_JOB, "false");
            properties.put("job.purge.period.time.ms", "300000");
            properties.put("job.pull.period.time.ms", "10000");
            properties.put("job.routing.period.time.ms", "50000");
            properties.put("job.push.period.time.ms", "10000");
            properties.put("job.heartbeat.period.time.ms", "300000");            
            properties.put(ParameterConstants.TRANSPORT_HTTP_TIMEOUT, "20000");
            properties.put(ParameterConstants.PURGE_RETENTION_MINUTES, "60");
            properties.put(ParameterConstants.STATISTIC_RECORD_ENABLE, "false");
            properties.put(ParameterConstants.ROUTING_PEEK_AHEAD_WINDOW, "100000");
            if (defaultProperties != null) {
                properties.putAll(defaultProperties);
            }
        } catch (IOException e) {
            throw new IoException(e);
        }
    }


    public TypedProperties reload() {
        return properties;
    }

}
