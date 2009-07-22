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

import java.io.IOException;
import java.util.Random;

import org.junit.Test;

public class MeteredOutputStreamTest {

    @Test
    public static void testBasic() throws IOException {
        final long rate = 5 * 1024;
        final long count = 20;
        final int bufferSize = 8192;

        MeteredOutputStream out = new MeteredOutputStream(new NullOutputStream(), rate, 8192, 1);
        long start = System.currentTimeMillis();
        byte[] testBytes = new byte[bufferSize];

        Random r = new Random();

        r.nextBytes(testBytes);

        long i = 0;
        for (i = 0; i < count; i++) {
            out.write(testBytes, 0, testBytes.length);

            if ((i % 10) == 0) {
                System.out.print('#');
            }
        }

        double expectedTime = (bufferSize * count) / rate;
        double actualTime = (System.currentTimeMillis() - start + 1) / 1000;
        assert (actualTime >= expectedTime - 2 && actualTime <= expectedTime + 2);
    }
}
