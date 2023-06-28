package org.jumpmind.symmetric.statistic;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import org.jumpmind.symmetric.model.DataGap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class RouterStatsTest {
    protected RouterStats test;

    @BeforeEach
    public void setUp() throws Exception {
        test = new RouterStats(2, 0, 0, 0, null, null);
    }

    @Test
    void testRouterStatsConstructor() throws Exception {
        assertEquals(2, test.getStartDataId());
        assertEquals(0, test.getEndDataId());
        assertEquals(0, test.getDataReadCount());
        assertEquals(0, test.getPeekAheadFillCount());
        assertEquals(null, test.getDataGaps());
    }

    @Test
    void testSetterMethods() throws Exception {
        test.setDataGaps(new ArrayList<DataGap>());
        test.setDataReadCount(5);
        test.setEndDataId(5);
        test.setPeekAheadFillCount(5);
        test.setStartDataId(5);
        assertEquals(5, test.getStartDataId());
        assertEquals(5, test.getEndDataId());
        assertEquals(5, test.getDataReadCount());
        assertEquals(5, test.getPeekAheadFillCount());
        assertEquals(new ArrayList<DataGap>(), test.getDataGaps());
    }

    @Test
    void testToString() throws Exception {
        String actualString = test.toString();
        String expectedString = "{ startDataId: " + "2" + ", endDataId: " + "0" + ", dataReadCount: " + "0" +
                ", peekAheadFillCount: " + "0" + ", dataGaps: null }";
        assertEquals(expectedString, actualString);
    }
}
