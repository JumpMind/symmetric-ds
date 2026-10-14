package org.jumpmind.symmetric.io.data.transform;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ValueMapColumnTransformTest {
    @Test
    public void test() {
        ValueMapColumnTransform transform = new ValueMapColumnTransform();
        TransformColumn column = new TransformColumn();
        column.setTransformExpression("unquoted_key_0=unquoted_value_0 "
                + "\"quoted key 1\"=unquoted_value_1 "
                + "unquoted_key_2=\"quoted value 2\" "
                + "\"quoted key 3\"=\"quoted value 3\" "
                + "\"\"\"key in escaped quotes 4\"\"\"=\"\"\"value in escaped quotes 4\"\"\" "
                + "\"equals=key 5\"=\"equals=value 5\" "
                + "*=\"default value 6\"");
        TransformedData data = new TransformedData(null, null, null, null, null);
        NewAndOldValue result = null;
        try {
            result = transform.transform(null, null, column, data, null, "unquoted_key_0", null);
            Assertions.assertEquals("unquoted_value_0", result.getNewValue(), "Failed to transform unquoted key into unquoted value");
            result = transform.transform(null, null, column, data, null, "quoted key 1", null);
            Assertions.assertEquals("unquoted_value_1", result.getNewValue(), "Failed to transform quoted key into unquoted value");
            result = transform.transform(null, null, column, data, null, "unquoted_key_2", null);
            Assertions.assertEquals("quoted value 2", result.getNewValue(), "Failed to transform unquoted key into quoted value");
            result = transform.transform(null, null, column, data, null, "quoted key 3", null);
            Assertions.assertEquals("quoted value 3", result.getNewValue(), "Failed to transform quoted key into quoted value");
            result = transform.transform(null, null, column, data, null, "\"key in escaped quotes 4\"", null);
            Assertions.assertEquals("\"value in escaped quotes 4\"",
                    result.getNewValue(), "Failed to transform key in escaped quotes into value in escaped quotes");
            result = transform.transform(null, null, column, data, null, "equals=key 5", null);
            Assertions.assertEquals("equals=value 5",
                    result.getNewValue(), "Failed to transform quoted key with equals sign into quoted value with equals sign");
            result = transform.transform(null, null, column, data, null, "key 6", null);
            Assertions.assertEquals("default value 6", result.getNewValue(), "Failed to transform key into default value");
            column.setTransformExpression("*=*");
            result = transform.transform(null, null, column, data, null, "key 7", null);
            Assertions.assertEquals("key 7", result.getNewValue(), "Failed to return the original key");
        } catch (Exception e) {
            Assertions.fail(e.getMessage());
        }
    }
}
