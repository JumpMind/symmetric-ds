package org.jumpmind.util;

import java.util.Date;

import org.apache.commons.lang.time.DateUtils;
import org.jumpmind.util.AppUtils;
import org.junit.Assert;
import org.junit.Test;

public class AppUtilsTest {

    @Test
    public void testGetLocalDateForOffset() {
        Date gmt = AppUtils.getLocalDateForOffset("+00:00");
        Date plusFour = AppUtils.getLocalDateForOffset("+04:00");
        Date minusFour = AppUtils.getLocalDateForOffset("-04:00");
        long nearZero = plusFour.getTime() - gmt.getTime() - DateUtils.MILLIS_PER_HOUR * 4;
        Assert.assertTrue(nearZero + " was the left over ms",  Math.abs(nearZero) < 1000);
        nearZero = plusFour.getTime() - minusFour.getTime() - DateUtils.MILLIS_PER_HOUR * 8;
        Assert.assertTrue(nearZero + " was the left over ms", Math.abs(nearZero) < 1000);
    }   
    
}