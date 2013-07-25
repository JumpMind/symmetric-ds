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

package org.jumpmind.symmetric.model;

/**
 * {@link TriggerHistory}
 */
public enum TriggerReBuildReason {

    NEW_TRIGGERS("N"),
    TABLE_SCHEMA_CHANGED("S"),
    TABLE_SYNC_CONFIGURATION_CHANGED("C"),
    FORCED("F"),
    TRIGGERS_MISSING("T"),
    TRIGGER_TEMPLATE_CHANGED("E");

    private String code;

    TriggerReBuildReason(String code) {
        this.code = code;
    }

    public String getCode() {
        return this.code;
    }

    public static TriggerReBuildReason fromCode(String code) {
        if (code != null && code.length() > 0) {
            if (code.equals(NEW_TRIGGERS.code)) {
                return NEW_TRIGGERS;
            } else if (code.equals(TABLE_SCHEMA_CHANGED.code)) {
                return TABLE_SCHEMA_CHANGED;
            } else if (code.equals(TABLE_SYNC_CONFIGURATION_CHANGED.code)) {
                return TABLE_SYNC_CONFIGURATION_CHANGED;
            } else if (code.equals(FORCED.code)) {
                return FORCED;
            } else if (code.equals(TRIGGER_TEMPLATE_CHANGED.code)) {
                return TRIGGER_TEMPLATE_CHANGED;
            }
        }
        return null;
    }

}