package org.jumpmind.symmetric.io.data.transform;

import java.util.Map;

import net.sourceforge.jeval.EvaluationException;
import net.sourceforge.jeval.Evaluator;

import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.extension.IBuiltInExtensionPoint;
import org.jumpmind.symmetric.io.data.DataContext;

public class MathColumnTransform implements ISingleValueColumnTransform, IBuiltInExtensionPoint {

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

    public String transform(IDatabasePlatform platform, DataContext context,
            TransformColumn column, TransformedData data, Map<String, String> sourceValues, String newValue, String oldValue) {

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
            return result;

        } catch (EvaluationException e) {
            throw new RuntimeException("Unable to evaluate transform expression: " + transformExpression);
        }
    }

}
