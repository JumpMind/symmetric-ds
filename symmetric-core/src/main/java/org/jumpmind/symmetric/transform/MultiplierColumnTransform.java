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

import java.util.List;
import java.util.Map;

import org.jumpmind.symmetric.ext.IBuiltInExtensionPoint;
import org.jumpmind.symmetric.ext.ICacheContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.SingleColumnRowMapper;
import org.springframework.jdbc.core.simple.SimpleJdbcTemplate;

public class MultiplierColumnTransform implements IMultipleValueColumnTransform, IBuiltInExtensionPoint {

    protected SimpleJdbcTemplate jdbcTemplate;

    public static final String NAME = "multiply";

    public boolean isAutoRegister() {
        return true;
    }

    public String getName() {
        return NAME;
    }

    public List<String> transform(ICacheContext context, TransformColumn column,
            TransformedData data, Map<String, String> sourceValues, String value, String oldValue)
            throws IgnoreColumnException, IgnoreRowException {
        return jdbcTemplate.query(column.getTransformExpression(), new SingleColumnRowMapper<String>(String.class) , sourceValues);
    }

    public void setJdbcTemplate(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = new SimpleJdbcTemplate(jdbcTemplate);
    }

}
