package org.jumpmind.symmetric.route;

import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.jumpmind.symmetric.model.DataGap;
import org.junit.Test;

public class DataGapDetectorTest {

    @Test
    public void testRemoveAbandonedGaps() {
        DataGapDetector detector = new DataGapDetector();
        List<DataGap> gaps = new ArrayList<DataGap>();
        
        gaps.add(new DataGap(1,1000));
        gaps.add(new DataGap(2000,3000));
        gaps.add(new DataGap(3001,3001));
        gaps.add(new DataGap(3002,3002));
        gaps.add(new DataGap(3003,3003));
        gaps.add(new DataGap(3004,3004));
        int expectedSize = gaps.size();
        
        List<DataGap> evaluatedList = detector.removeAbandonedGaps(gaps);
        Assert.assertEquals(expectedSize, evaluatedList.size());
        
        gaps.add(new DataGap(2000,2001));
        gaps.add(new DataGap(2010,2022));        
        gaps.add(new DataGap(2899,3000));
        
        Assert.assertTrue(gaps.size() > expectedSize);
        
        evaluatedList = detector.removeAbandonedGaps(gaps);
        Assert.assertEquals(expectedSize, evaluatedList.size());
        
        
    }
}
