package org.jumpmind.symmetric.io.data.transform;

import static java.util.Arrays.asList;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.core.IsNull.nullValue;
import static org.jumpmind.symmetric.io.data.DataEventType.UPDATE;
import static org.jumpmind.symmetric.io.data.transform.CopyColumnTransform.NAME;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.powermock.reflect.Whitebox.invokeMethod;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest({TransformedData.class})
public class TransformedDataTest {

    public static final String ADD_VALUE_METHOD = "addOldValue";
    private TransformedData transformedData;
    private TransformTable transformation;
    private HashMap<String, String> sourceKeyValues;
    private HashMap<String, String> oldSourceValues;
    private HashMap<String, String> sourceValues;
    private List<String> keyNames;
    private List<String> keyValues;
    private List<String> values;
    public static final String COLUMN_VALUE = String.valueOf(Integer.MAX_VALUE);
    private TransformColumn transformColumn;
    public static final String TARGET_NAME = "code_store_table";
    public static final String SOURCE_NAME = "code_corp_table";

    @Before
    public void setUp() throws Exception {

        keyNames = new ArrayList<String>();
        keyValues = new ArrayList<String>();
        values = new ArrayList<String>();

        transformedData = new TransformedData(
                transformation = mock(TransformTable.class),
                UPDATE,
                sourceKeyValues = new HashMap<String, String>(),
                oldSourceValues = new HashMap<String, String>(),
                sourceValues = new HashMap<String, String>()
        );

        transformColumn = mock(TransformColumn.class);
    }

    @SuppressWarnings("rawtypes")
    @After
    public void tearDown() throws Exception {

        for (Collection c : new Collection[]{keyNames, keyValues, values}) {
            c.clear();
        }

        for (Map m : new Map[]{sourceKeyValues, oldSourceValues, sourceValues}) {
            m.clear();
        }
    }

    @Test
    public void addKeyValue() throws Exception {

        String name = "id_store_table";
        String value = String.valueOf(Integer.MAX_VALUE);

        keyNames.add(name);
        keyValues.add(value);

        invokeMethod(transformedData, ADD_VALUE_METHOD, keyNames, keyValues, values, name);

        assertThat(values.iterator().next(), equalTo(value));
    }

    @Test
    public void addValueWithoutTransformation() throws Exception {

        String name = "column_same_name_on_source_and_target_nodes";

        oldSourceValues.put(name, COLUMN_VALUE);

        invokeMethod(transformedData, ADD_VALUE_METHOD, keyNames, keyValues, values, name);

        assertThat(values.iterator().next(), equalTo(COLUMN_VALUE));
    }

    @Test
    public void addNoValueWithoutTransformation() throws Exception {

        String name = "column_same_name_on_source_and_target_nodes";
        invokeMethod(transformedData, ADD_VALUE_METHOD, keyNames, keyValues, values, name);

        assertThat(values.iterator().next(), nullValue());
    }

    @Test
    public void addValueCopy() throws Exception {

        setTransformColumn(TARGET_NAME, SOURCE_NAME, NAME);

        oldSourceValues.put(SOURCE_NAME, COLUMN_VALUE);

        invokeMethod(transformedData, ADD_VALUE_METHOD, keyNames, keyValues, values, TARGET_NAME);

        assertThat(values.iterator().next(), equalTo(COLUMN_VALUE));
    }

    @Test
    public void removeTransformedValue() throws Exception {

        setTransformColumn(TARGET_NAME, SOURCE_NAME, "remove");

        oldSourceValues.put(SOURCE_NAME, COLUMN_VALUE);

        invokeMethod(transformedData, ADD_VALUE_METHOD, keyNames, keyValues, values, TARGET_NAME);

        assertThat(values.iterator().next(), nullValue());
    }

    private void setTransformColumn(String targetName, String sourceName, String transformType) {

        when(transformColumn.getTargetColumnName()).thenReturn(targetName);
        when(transformColumn.getTransformType()).thenReturn(transformType);
        when(transformColumn.getSourceColumnName()).thenReturn(sourceName);
        when(transformation.getTransformColumns()).thenReturn(asList(transformColumn));
    }
}
