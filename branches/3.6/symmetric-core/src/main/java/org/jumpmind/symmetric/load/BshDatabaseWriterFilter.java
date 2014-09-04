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
package org.jumpmind.symmetric.load;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jumpmind.db.model.Table;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.SymmetricException;
import org.jumpmind.symmetric.io.data.CsvData;
import org.jumpmind.symmetric.io.data.DataContext;
import org.jumpmind.symmetric.io.data.DataEventType;
import org.jumpmind.symmetric.model.LoadFilter;
import org.jumpmind.util.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import bsh.EvalError;
import bsh.Interpreter;
import bsh.TargetError;

public class BshDatabaseWriterFilter extends DynamicDatabaseWriterFilter {

    private static final String OLD_ = "OLD_";
    private static final String CONTEXT = "context";
    private static final String TABLE = "table";
    private static final String DATA = "data";
    private static final String ERROR = "error";
    private static final String ENGINE = "engine";
    private static final String LOG = "log";
    private final String INTERPRETER_KEY = String.format("%d.BshInterpreter", hashCode());

    protected final Logger log = LoggerFactory.getLogger(getClass());

    public BshDatabaseWriterFilter(ISymmetricEngine engine,
            Map<String, List<LoadFilter>> loadFilters) {
        super(engine, loadFilters);
    }

    @Override
    protected boolean processLoadFilters(DataContext context, Table table, CsvData data,
            Exception error, WriteMethod writeMethod, List<LoadFilter> loadFiltersForTable) {

        boolean writeRow = true;
        LoadFilter currentFilter = null;

        try {
            Interpreter interpreter = getInterpreter(context);
            bind(interpreter, context, table, data, error);
            for (LoadFilter filter : loadFiltersForTable) {
                currentFilter = filter;
                if (filter.isFilterOnDelete()
                        && data.getDataEventType().equals(DataEventType.DELETE)
                        || filter.isFilterOnInsert()
                        && data.getDataEventType().equals(DataEventType.INSERT)
                        || filter.isFilterOnUpdate()
                        && data.getDataEventType().equals(DataEventType.UPDATE)) {
                    Object result = null;
                    if (writeMethod.equals(WriteMethod.BEFORE_WRITE)
                            && filter.getBeforeWriteScript() != null) {
                        result = interpreter.eval(filter.getBeforeWriteScript());
                    } else if (writeMethod.equals(WriteMethod.AFTER_WRITE)
                            && filter.getAfterWriteScript() != null) {
                        result = interpreter.eval(filter.getAfterWriteScript());
                    } else if (writeMethod.equals(WriteMethod.HANDLE_ERROR)
                            && filter.getHandleErrorScript() != null) {
                        result = interpreter.eval(filter.getHandleErrorScript());
                    }

                    if (result != null && result.equals(Boolean.FALSE)) {
                        writeRow = false;
                    }
                }
            }
        } catch (EvalError ex) {
            processError(currentFilter, table, ex);
        }

        return writeRow;
    }

    @Override
    protected void executeScripts(DataContext context, String key, Set<String> scripts, boolean isFailOnError) {
        Interpreter interpreter = getInterpreter(context);
        String currentScript = null;
        try {
            bind(interpreter, context, null, null, null);
            if (scripts != null) {
                    for (String script : scripts) {
                        currentScript = script;
                        interpreter.eval(script);
                    }
            }
        } catch (EvalError e) {
            String errorMsg = String.format("Beanshell script %s with error %s", new Object[] {
                    currentScript, e.getErrorText() });
            log.error(errorMsg);
            if (isFailOnError) {
                throw new SymmetricException(errorMsg);
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

    protected void bind(Interpreter interpreter, DataContext context, Table table, CsvData data, Exception error)
            throws EvalError {

        interpreter.set(LOG, log);
        interpreter.set(ENGINE, this.engine);
        interpreter.set(CONTEXT, context);
        interpreter.set(TABLE, table);
        interpreter.set(DATA, data);
        interpreter.set(ERROR, error);        

        if (data != null) {
            Map<String, String> sourceValues = data.toColumnNameValuePairs(table.getColumnNames(),
                    CsvData.ROW_DATA);
            for (String columnName : sourceValues.keySet()) {
                interpreter.set(columnName, sourceValues.get(columnName));
                interpreter.set(columnName.toUpperCase(), sourceValues.get(columnName));
            }

            Map<String, String> oldValues = data.toColumnNameValuePairs(table.getColumnNames(),
                    CsvData.OLD_DATA);
            for (String columnName : oldValues.keySet()) {
                interpreter.set(OLD_ + columnName, sourceValues.get(columnName));
                interpreter.set(OLD_ + columnName.toUpperCase(), sourceValues.get(columnName));
            }
        }

    }

    protected void processError(LoadFilter currentFilter, Table table, Throwable ex) {
        if (ex instanceof TargetError) {
            ex = ((TargetError) ex).getTarget();
        }
        String formattedMessage = String.format(
                "Error executing beanshell script for load filter %s on table %s. The error was: %s",
                new Object[] { currentFilter != null ? currentFilter.getLoadFilterId() : "N/A",
                        table.getName(), ex.getMessage() });
        log.error(formattedMessage);
        if (currentFilter.isFailOnError()) {
            throw new SymmetricException(formattedMessage, ex);
        }
    }
}
