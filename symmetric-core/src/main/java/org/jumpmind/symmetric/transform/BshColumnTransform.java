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

import java.util.Map;

import org.jumpmind.symmetric.common.logging.ILog;
import org.jumpmind.symmetric.common.logging.LogFactory;
import org.jumpmind.symmetric.ext.IBuiltInExtensionPoint;
import org.jumpmind.symmetric.ext.ICacheContext;
import org.springframework.jdbc.core.JdbcTemplate;

import bsh.Interpreter;
import bsh.TargetError;

public class BshColumnTransform implements ISingleValueColumnTransform, IBuiltInExtensionPoint {

    protected final ILog log = LogFactory.getLog(getClass());
    
    final String INTERPRETER_KEY = String.format("%d.BshInterpreter", hashCode());

    public static final String NAME = "bsh";
    
    protected JdbcTemplate jdbcTemplate;
    
    public boolean isAutoRegister() {
        return true;
    }

    public String getName() {
        return NAME;
    }

    public String transform(ICacheContext context, TransformColumn column,
            TransformedData data, Map<String, String> sourceValues, String value, String oldValue)
            throws IgnoreColumnException, IgnoreRowException {
        try {
            Interpreter interpreter = getInterpreter(context);
            interpreter.set("jdbcTemplate", jdbcTemplate);
            interpreter.set("currentValue", value);
            interpreter.set("oldValue", oldValue);
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
                throw (IgnoreColumnException)ex;
            } else if (ex instanceof IgnoreRowException) {
                throw (IgnoreRowException)ex;
            } else {
                throw new TransformColumnException(ex);
            }
        } catch (Exception ex) {
            if (ex instanceof IgnoreColumnException) {
                throw (IgnoreColumnException)ex;
            } else if (ex instanceof IgnoreRowException) {
                throw (IgnoreRowException)ex;
            } else {
                log.error("BshColumnTransform.exception", column.getTargetColumnName(), column.getTransformId());
                throw new TransformColumnException(ex);
            }
        }
    }

    protected Interpreter getInterpreter(ICacheContext context) {
        Interpreter interpreter = (Interpreter) context.getContextCache().get(INTERPRETER_KEY);
        if (interpreter == null) {
            interpreter = new Interpreter();
            context.getContextCache().put(INTERPRETER_KEY, interpreter);
        }
        return interpreter;
    }
    
    public void setJdbcTemplate(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }
}
