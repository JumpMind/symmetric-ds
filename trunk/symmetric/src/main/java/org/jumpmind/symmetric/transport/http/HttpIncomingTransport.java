package org.jumpmind.symmetric.transport.http;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;

import org.apache.commons.io.IOUtils;
import org.jumpmind.symmetric.service.RegistrationNotOpenException;
import org.jumpmind.symmetric.transport.IIncomingTransport;
import org.jumpmind.symmetric.web.WebConstants;

public class HttpIncomingTransport implements IIncomingTransport {

    private HttpURLConnection conn;

    private BufferedReader reader;

    public HttpIncomingTransport(HttpURLConnection connection) {
        this.conn = connection;
    }

    public void close() throws IOException {
        IOUtils.closeQuietly(reader);
    }

    public boolean isOpen() {
        return reader != null;
    }

    public BufferedReader open() throws IOException {
        if (WebConstants.REGISTRATION_NOT_OPEN == conn.getResponseCode()) {
            throw new RegistrationNotOpenException();
        } else {
        reader = new BufferedReader(
                new InputStreamReader(conn.getInputStream(), "UTF-8"));
        return reader;
        }
    }
}
