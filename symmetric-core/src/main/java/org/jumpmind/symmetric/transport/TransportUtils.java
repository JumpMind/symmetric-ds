package org.jumpmind.symmetric.transport;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;

import org.jumpmind.exception.IoException;
import org.jumpmind.symmetric.io.IoConstants;

public class TransportUtils {

    public static BufferedReader toReader(InputStream is) {
        try {
            return new BufferedReader(new InputStreamReader(is, IoConstants.ENCODING));
        } catch (IOException ex) {
            throw new IoException(ex);
        }
    }

    public static BufferedWriter toWriter(OutputStream os) {
        try {
            return new BufferedWriter(new OutputStreamWriter(os, IoConstants.ENCODING));
        } catch (IOException ex) {
            throw new IoException(ex);
        }

    }
}