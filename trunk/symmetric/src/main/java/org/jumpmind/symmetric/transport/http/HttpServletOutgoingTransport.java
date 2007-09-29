package org.jumpmind.symmetric.transport.http;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;

import javax.servlet.http.HttpServletResponse;

import org.jumpmind.symmetric.transport.IOutgoingTransport;

public class HttpServletOutgoingTransport implements IOutgoingTransport {
private HttpServletResponse response;

    public HttpServletOutgoingTransport(HttpServletResponse resp) {
        this.response = resp;
    }

    public void close() throws IOException {
    }

    public BufferedWriter open() throws IOException {
        return new BufferedWriter(response.getWriter());
    }

    public BufferedReader readResponse() throws IOException {
        throw new UnsupportedOperationException();
    }

    public boolean isOpen() {
        return response != null;
    }

}
