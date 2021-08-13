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
package org.jumpmind.symmetric.util;

import java.util.Properties;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings("NM_SAME_SIMPLE_NAME_AS_SUPERCLASS")
public class PropertiesFactoryBean extends org.springframework.beans.factory.config.PropertiesFactoryBean {
    private static Properties localProperties;

    public PropertiesFactoryBean() {
        this.setLocalOverride(true);
        if (localProperties != null) {
            this.setProperties(localProperties);
        }
    }

    public static void setLocalProperties(Properties localProperties) {
        PropertiesFactoryBean.localProperties = localProperties;
    }

    public static void clearLocalProperties() {
        PropertiesFactoryBean.localProperties = null;
    }
}
