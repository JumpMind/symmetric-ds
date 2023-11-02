package org.jumpmind.symmetric.service.impl;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.jumpmind.symmetric.model.DataGap;
import org.junit.jupiter.api.Test;

public class PurgeServiceTest {
    @Test
    public void testPurgeAroundSmallGaps() {
        List<DataGap> gaps = new ArrayList<DataGap>();
        gaps.add(new DataGap(1821, 1824));
        gaps.add(new DataGap(1838, 1838));
        long[] minMax = PurgeService.getMinMaxAvoidGaps(1821, 1846, gaps);
        assertEquals(1825, minMax[0]);
        assertEquals(1837, minMax[1]);
        assertEquals(gaps.size(), 1);
        minMax = PurgeService.getMinMaxAvoidGaps(1838, 1838, gaps);
        assertEquals(-1839, minMax[1]);
        assertEquals(gaps.size(), 1);
        minMax = PurgeService.getMinMaxAvoidGaps(1838, 1846, gaps);
        assertEquals(1839, minMax[0]);
        assertEquals(1846, minMax[1]);
        assertEquals(gaps.size(), 0);
    }
}
