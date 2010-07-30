/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Chris Henson <chenson42@users.sourceforge.net>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see
 * <http://www.gnu.org/licenses/>.
 */
package org.jumpmind.symmetric.util;

import java.util.Date;

import org.apache.commons.lang.time.DateUtils;
import org.junit.Assert;
import org.junit.Test;

public class AppUtilsUnitTest {

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
