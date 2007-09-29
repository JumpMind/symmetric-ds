package org.jumpmind.symmetric.transport;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class TransportUtils {

    public static BufferedReader toReader(InputStream is) throws IOException {
        return new BufferedReader(
                new InputStreamReader(is, "UTF-8"));
    }
}
