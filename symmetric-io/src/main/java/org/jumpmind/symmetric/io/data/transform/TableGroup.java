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
package org.jumpmind.symmetric.io.data.transform;

import java.util.Date;

public class TableGroup implements Cloneable {
    private String id;
    private WriterType writerType;
    private Date createTime;
    private Date lastUpdateTime;
    private String lastUpdateBy;

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof TableGroup && id != null) {
            return id.equals(((TableGroup) obj).id);
        }
        return false;
    }

    @Override
    public int hashCode() {
        if (id != null) {
            return id.hashCode();
        }
        return super.hashCode();
    }

    @Override
    public String toString() {
        if (id != null) {
            return id;
        }
        return super.toString();
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public WriterType getWriterType() {
        return writerType;
    }

    public void setWriterType(WriterType writerType) {
        this.writerType = writerType;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public Date getLastUpdateTime() {
        return lastUpdateTime;
    }

    public String getLastUpdateBy() {
        return lastUpdateBy;
    }

    public void setCreateTime(Date createdOn) {
        this.createTime = createdOn;
    }

    public void setLastUpdateTime(Date lastModifiedOn) {
        this.lastUpdateTime = lastModifiedOn;
    }

    public void setLastUpdateBy(String updatedBy) {
        this.lastUpdateBy = updatedBy;
    }

    public enum WriterType {
        DEFAULT("D"), JSON("J");

        private String code;

        WriterType(String code) {
            this.code = code;
        }

        public String getCode() {
            return code;
        }

        public static WriterType getWriterType(String s) {
            if (s.equals(JSON.getCode())) {
                return JSON;
            } else {
                return DEFAULT;
            }
        }
    }
}