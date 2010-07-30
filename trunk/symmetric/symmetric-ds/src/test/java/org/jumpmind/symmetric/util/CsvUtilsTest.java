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

import junit.framework.Assert;

import org.junit.Test;

public class CsvUtilsTest {

    @Test
    public void testLastElementIsNull() {
        String[] tokens = CsvUtils.tokenizeCsvData("\"01493931\",\"0\",\"01493931\",,\"UktNQzIxMAD/////AAAABXV1aWQAAAAAEG3jUZmt5UvpiUdFsbLEkJT/////AAAAA2l2AAAAABClDAHak0h0ENSr3PGH8qIU/////wAAAAVjc3VtAAAAACBjkvrLppvDkY1EbaURqm2kpvmcg/j9eUxztrCe4JHXpH0suPSRvgP6LtpJMaH1HZc=\",\"Trevor Lewis\",\"Lewis\",\"Trevorr\",,\"02\",,\"1\",\"16\",,\"0\",,\"0\",\"30683\",\"0\",\"2010-05-18 15:43:21\",\"0\",\"1987-09-24 00:00:00\",\"0\",,\"PS\",\"2009-11-14 21:49:35\",\"2009-11-14 21:49:35\",\"0023\",,\"1\",");
        String expectedNull = tokens[tokens.length-1];
        Assert.assertNull("Expected null.  Instead received: " + expectedNull, expectedNull);
    }
}
