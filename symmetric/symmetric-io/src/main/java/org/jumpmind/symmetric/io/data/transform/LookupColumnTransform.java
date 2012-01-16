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

package org.jumpmind.symmetric.io.data.transform;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.mapper.StringMapper;
import org.jumpmind.extension.IBuiltInExtensionPoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.jumpmind.symmetric.io.data.DataContext;
import org.jumpmind.symmetric.io.data.IDataReader;
import org.jumpmind.symmetric.io.data.IDataWriter;

public class LookupColumnTransform implements ISingleValueColumnTransform, IBuiltInExtensionPoint {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    public static final String NAME = "lookup";

    protected static final StringMapper lookupColumnRowMapper = new StringMapper();

    public boolean isAutoRegister() {
        return true;
    }

    public String getName() {
        return NAME;
    }

    public boolean isExtractColumnTransform() {
        return true;
    }

    public boolean isLoadColumnTransform() {
        return true;
    }

    public String transform(IDatabasePlatform platform,
            DataContext<? extends IDataReader, ? extends IDataWriter> context,
            TransformColumn column, TransformedData data, Map<String, String> sourceValues,
            String newValue, String oldValue) throws IgnoreColumnException, IgnoreRowException {
        String sql = column.getTransformExpression();
        String lookupValue = null;

        if (StringUtils.isNotBlank(sql)) {
            List<String> values = platform.getSqlTemplate().query(sql, lookupColumnRowMapper,
                    sourceValues);
            int rowCount = values.size();

            if (rowCount == 1) {
                lookupValue = values.get(0);
            } else if (rowCount > 1) {
                lookupValue = values.get(0);
                log.warn("Expected a single row, but returned multiple rows from lookup for target column %s on transform %s ", column.getTargetColumnName(),
                        column.getTransformId());
            } else if (values.size() == 0) {
                log.warn("Expected a single row, but returned no rows from lookup for target column %s on transform %s", column.getTargetColumnName(),
                        column.getTransformId());
            }
        } else {
            log.warn("Expected SQL expression for lookup transform, but no expression was found for target column %s on transform %s", column.getTargetColumnName(),
                    column.getTransformId());
        }
        return lookupValue;
    }

}
