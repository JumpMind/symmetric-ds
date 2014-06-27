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
package org.jumpmind.symmetric.service.impl;

import java.util.Map;

import org.jumpmind.db.platform.IDatabasePlatform;

public class SequenceServiceSqlMap extends AbstractSqlMap {

    public SequenceServiceSqlMap(IDatabasePlatform platform, Map<String, String> replacementTokens) {
        super(platform, replacementTokens);

        // @formatter:off
        putSql("getSequenceSql",
          "select sequence_name,current_value,increment_by,min_value,max_value,                    " + 
          "cycle,create_time,last_update_by,last_update_time from $(sequence) where sequence_name=?");
        
        putSql("getCurrentValueSql",
                "select current_value from $(sequence) where sequence_name=?");     
        
        putSql("updateCurrentValueSql",
                "update $(sequence) set current_value=?, last_update_time=current_timestamp " +
                "  where sequence_name=? and current_value=?                                ");          
        
        putSql("insertSequenceSql",
                "insert into $(sequence)                                               " +
                "  (sequence_name, current_value, increment_by, min_value, max_value,  " + 
                "   cycle, create_time, last_update_by, last_update_time)              " +
                "   values(?,?,?,?,?,?,current_timestamp,?,current_timestamp)         ");     
        
        putSql("maxOutgoingBatchSql", "select max(batch_id)+1 from $(outgoing_batch)");
                
       // @formatter:on
    }

}
