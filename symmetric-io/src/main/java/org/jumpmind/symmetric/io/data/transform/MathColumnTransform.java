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

import net.sourceforge.jeval.EvaluationException;
import net.sourceforge.jeval.Evaluator;

import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.extension.IBuiltInExtensionPoint;
import org.jumpmind.symmetric.io.data.DataContext;
import org.jumpmind.symmetric.io.data.DataEventType;

public class MathColumnTransform implements ISingleNewAndOldValueColumnTransform, IBuiltInExtensionPoint {

    public final static String NAME = "math";

    private static Evaluator eval = new Evaluator();

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

        String transformExpression = column.getTransformExpression();
        try {
            eval.clearVariables();
            eval.putVariable("currentValue", newValue);
            eval.putVariable("oldValue", oldValue);
            eval.putVariable("channelId", context.getBatch().getChannelId());

            for (String columnName : sourceValues.keySet()) {
                eval.putVariable(columnName.toUpperCase(), sourceValues.get(columnName));
                eval.putVariable(columnName, sourceValues.get(columnName));
            }
            
            // JEval always returns a double with at least one decimal place. 
            // Truncate the decimal place if not needed so the number can be inserted into an integer column.
            String result = eval.evaluate(transformExpression);
            Double dblResult = Double.valueOf(result);
            if (dblResult == Math.floor(dblResult)) {
                result = result.substring(0, result.length()-2);
            }

            if (data.getTargetDmlType().equals(DataEventType.DELETE)) {
                return new NewAndOldValue(null, result);
            } else {
                return new NewAndOldValue(result, null);
            }
        } catch (EvaluationException e) {
            throw new RuntimeException("Unable to evaluate transform expression: " + transformExpression);
        }
    }

}
