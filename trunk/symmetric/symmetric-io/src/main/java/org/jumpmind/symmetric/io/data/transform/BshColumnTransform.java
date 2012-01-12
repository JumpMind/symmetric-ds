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

import java.util.Map;

import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.extension.IBuiltInExtensionPoint;
import org.jumpmind.log.Log;
import org.jumpmind.log.LogFactory;
import org.jumpmind.symmetric.io.data.DataContext;
import org.jumpmind.symmetric.io.data.IDataReader;
import org.jumpmind.symmetric.io.data.IDataWriter;
import org.jumpmind.util.Context;

import bsh.Interpreter;
import bsh.TargetError;

public class BshColumnTransform implements ISingleValueColumnTransform, IBuiltInExtensionPoint {

    protected final Log log = LogFactory.getLog(getClass());

    final String INTERPRETER_KEY = String.format("%d.BshInterpreter", hashCode());

    public static final String NAME = "bsh";

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
        try {
            Interpreter interpreter = getInterpreter(context);
            interpreter.set("sqlTemplate", platform.getSqlTemplate());
            interpreter.set("currentValue", newValue);
            interpreter.set("oldValue", oldValue);
            interpreter.set("sourceNodeId", context.getBatch().getSourceNodeId());
            interpreter.set("channelId", context.getBatch().getChannelId());
            for (String columnName : sourceValues.keySet()) {
                interpreter.set(columnName.toUpperCase(), sourceValues.get(columnName));
            }
            Object result = interpreter.eval(column.getTransformExpression());
            if (result != null) {
                return result.toString();
            } else {
                return null;
            }
        } catch (TargetError evalEx) {
            Throwable ex = evalEx.getTarget();
            if (ex instanceof IgnoreColumnException) {
                throw (IgnoreColumnException) ex;
            } else if (ex instanceof IgnoreRowException) {
                throw (IgnoreRowException) ex;
            } else {
                throw new TransformColumnException(ex);
            }
        } catch (Exception ex) {
            if (ex instanceof IgnoreColumnException) {
                throw (IgnoreColumnException) ex;
            } else if (ex instanceof IgnoreRowException) {
                throw (IgnoreRowException) ex;
            } else {
                log.error("Beanshell script error for target column %s on transform %s", column.getTargetColumnName(),
                        column.getTransformId());
                throw new TransformColumnException(ex);
            }
        }
    }

    protected Interpreter getInterpreter(Context context) {
        Interpreter interpreter = (Interpreter) context.get(INTERPRETER_KEY);
        if (interpreter == null) {
            interpreter = new Interpreter();
            context.put(INTERPRETER_KEY, interpreter);
        }
        return interpreter;
    }

}
