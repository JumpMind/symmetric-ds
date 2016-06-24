package org.jumpmind.symmetric.model;

import static org.junit.Assert.assertArrayEquals;

import java.sql.Types;

import org.jumpmind.db.model.Column;
import org.junit.Test;

public class TriggerTest {

    @Test
    public void testFilterExcludedColumnsForNull() {
        Trigger trigger = new Trigger();
        assertArrayEquals("Expected empty column array", new Column[0], trigger.filterExcludedColumns(null));        
    }
    
    @Test
    public void testFilterExcludedColumns() {
        Trigger trigger = new Trigger();
        trigger.setExcludedColumnNames("col1, col3, col5");
        Column[] param = buildColumnArray();
        Column[] expected = new Column[] { param[1], param[3], param[5] };
        assertArrayEquals("Excluded wrong set of columns", expected, 
                trigger.filterExcludedColumns(param));        
    }
    
    @Test
    public void testFilterIncludedColumnsForNull() {
        Trigger trigger = new Trigger();
        assertArrayEquals("Expected empty column array", new Column[0], trigger.filterIncludedColumns(null));        
    }
    
    @Test
    public void testFilterIncludedColumns() {
        Trigger trigger = new Trigger();
        trigger.setIncludedColumnNames("col1, col3, col5");
        Column[] param = buildColumnArray();
        Column[] expected = new Column[] { param[0], param[2], param[4] };
        assertArrayEquals("Included wrong set of columns", expected, 
                trigger.filterIncludedColumns(param));        
    }
    
    @Test
    public void testFilterExcludedAndIncludedColumnsMutuallyExclusive() {
        Trigger trigger = new Trigger();
        trigger.setIncludedColumnNames("col1, col2");
        trigger.setExcludedColumnNames("col3, col4");
        Column[] param = buildColumnArray();
        Column[] expected = new Column[] { param[0], param[1] };
        assertArrayEquals("Filtered wrong set of columns", expected, 
                trigger.filterExcludedAndIncludedColumns(param));        
    }
    
    @Test
    public void testFilterExcludedAndIncludedColumnsOverlap() {
        Trigger trigger = new Trigger();
        trigger.setIncludedColumnNames("col1, col2");
        trigger.setExcludedColumnNames("col2, col3");
        Column[] param = buildColumnArray();
        Column[] expected = new Column[] { param[0] };
        assertArrayEquals("Filtered wrong set of columns", expected, 
                trigger.filterExcludedAndIncludedColumns(param));        
    }
    
    @Test
    public void testFilterExcludedAndIncludedColumnsExcludeOnly() {
        Trigger trigger = new Trigger();
        trigger.setExcludedColumnNames("col3, col4");
        trigger.setIncludedColumnNames("");
        
        Column[] param = buildColumnArray();
        Column[] expected = new Column[] { param[0], param[1], param[4], param[5] };
        assertArrayEquals("Filtered wrong set of columns", expected, 
                trigger.filterExcludedAndIncludedColumns(param));        
    }
    
    @Test
    public void testFilterExcludedAndIncludedColumnsIncludeOnly() {
        Trigger trigger = new Trigger();
        trigger.setExcludedColumnNames("");
        trigger.setIncludedColumnNames("col1, col2");
        
        Column[] param = buildColumnArray();
        Column[] expected = new Column[] { param[0], param[1] };
        assertArrayEquals("Filtered wrong set of columns", expected, 
                trigger.filterExcludedAndIncludedColumns(param));        
    }
    
    public Column[] buildColumnArray() {
        Column c1 = new Column("col1", false, Types.INTEGER, 50, 0);
        Column c2 = new Column("col2", false, Types.INTEGER, 50, 0);
        Column c3 = new Column("col3", false, Types.INTEGER, 50, 0);
        Column c4 = new Column("col4", false, Types.INTEGER, 50, 0);
        Column c5 = new Column("col5", false, Types.INTEGER, 50, 0);
        Column c6 = new Column("col6", false, Types.INTEGER, 50, 0);
        
        return new Column[] { c1, c2, c3, c4, c5, c6 };
    }
}
