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

package org.jumpmind.symmetric.transform;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.symmetric.common.logging.ILog;
import org.jumpmind.symmetric.common.logging.LogFactory;
import org.jumpmind.symmetric.ext.IBuiltInExtensionPoint;
import org.jumpmind.symmetric.ext.ICacheContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.SimpleJdbcTemplate;

public class LookupColumnTransform implements ISingleValueColumnTransform, IBuiltInExtensionPoint {

    protected final ILog log = LogFactory.getLog(getClass());
    
    public static final String NAME = "lookup";
    
    protected static final LookupColumnRowMapper lookupColumnRowMapper = new LookupColumnRowMapper();

    public boolean isAutoRegister() {
        return true;
    }

    public String getName() {
        return NAME;
    }

    public String transform(ICacheContext context, TransformColumn column, TransformedData data,
        Map<String, String> sourceValues, String value, String oldValue) throws IgnoreColumnException,
        IgnoreRowException {
        String sql = column.getTransformExpression();
        String lookupValue = null;
        
        if (StringUtils.isNotBlank(sql)) {
            SimpleJdbcTemplate jdbcTemplate = new SimpleJdbcTemplate(context.getJdbcTemplate());
            List<String> values = jdbcTemplate.query(sql, lookupColumnRowMapper, sourceValues);
            int rowCount = values.size();
            
            if (rowCount == 1) {
                lookupValue = values.get(0);
            } else if (rowCount > 1) {
                lookupValue = values.get(0);
                log.warn("LookupColumnTransform.multipleRows", column.getTargetColumnName(), column.getTransformId());
            } else if (values.size() == 0) {
                log.warn("LookupColumnTransform.noRows", column.getTargetColumnName(), column.getTransformId());
            }
        } else {
            log.warn("LookupColumnTransform.noSql", column.getTargetColumnName(), column.getTransformId());
        }
        return lookupValue;
    }

    static class LookupColumnRowMapper implements RowMapper<String> {
        public String mapRow(ResultSet rs, int rowNum) throws SQLException {
            return rs.getString(1);
        }
    }

}
