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

import org.jumpmind.symmetric.util.MeteredOutputStream;
import org.testng.annotations.Test;

public class MeteredOutputStreamTest
{

    @Test
    public void basicTest() throws IOException
    {
        final long rate = 56 * MeteredOutputStream.KB;
        final long configuredTime = 100;
        
        System.out.println("Running for " + configuredTime + " seconds.");

        MeteredOutputStream out = new MeteredOutputStream(new NullOutputStream(), rate);
        long start = System.currentTimeMillis();

        long i = 0;
        for (i = 0; i < (rate * configuredTime); i++)
        {
            out.write(65);
            
            if ((i % rate) == 0) 
            {
                System.out.print('.');
            }
        }
        System.out.println();

        long actualTime = (System.currentTimeMillis() - start) / 1000;
        System.out.println("Configured rate: " + rate);
        System.out.println("Actual rate: " + (i / actualTime));
        System.out.println(actualTime);
        System.out.println(configuredTime);

        assert (actualTime >= configuredTime - 2 && actualTime <= configuredTime + 2);
    }
}
