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

import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.extension.IBuiltInExtensionPoint;
import org.jumpmind.symmetric.io.data.DataContext;
import org.jumpmind.symmetric.io.data.DataEventType;

public class BinaryLeftColumnTransform implements ISingleNewAndOldValueColumnTransform, IBuiltInExtensionPoint {

    public static final String NAME = "bleft";

    public String getName() {
        return NAME;
    }
    
    public boolean isExtractColumnTransform() {
        return true;
    }
    
    public boolean isLoadColumnTransform() {
        return true;
    }

    public NewAndOldValue transform(IDatabasePlatform platform,
            DataContext context,
            TransformColumn column, TransformedData data, Map<String, String> sourceValues,
            String newValue, String oldValue) throws IgnoreColumnException, IgnoreRowException {

        if (StringUtils.isNotBlank(newValue)) {
            String expression = column.getTransformExpression();
            if (StringUtils.isNotBlank(expression)) {
                expression = expression.trim();
                newValue = bleft(newValue, Integer.parseInt(expression));
            }
        }
        
        if (data.getTargetDmlType().equals(DataEventType.DELETE)) {
            return new NewAndOldValue(null, newValue);
        } else {
            return new NewAndOldValue(newValue, null);
        }
    }
    
    public String bleft(String value, int maxBytes) {
        StringBuilder ret = new StringBuilder();
        for(int i = 0;i < value.length(); i++) {
            if((maxBytes -= value.substring(i, i+1).getBytes().length) < 0) break;
            ret.append(value.charAt(i));
        }
        return ret.toString();
     }

}
