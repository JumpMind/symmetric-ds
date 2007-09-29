
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
