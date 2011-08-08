package org.jumpmind.symmetric.transform;

import java.util.Map;

import org.jumpmind.symmetric.ext.IBuiltInExtensionPoint;
import org.jumpmind.symmetric.ext.ICacheContext;

import bsh.Interpreter;

public class BshColumnTransform implements ISingleValueColumnTransform, IBuiltInExtensionPoint {

    final String INTERPRETER_KEY = String.format("%d.BshInterpreter", hashCode());

    public static final String NAME = "bsh";
    
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
        } catch (Exception ex) {
            throw new TransformColumnException(ex);
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
}
