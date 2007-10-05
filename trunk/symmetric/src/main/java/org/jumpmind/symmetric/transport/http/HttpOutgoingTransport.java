package org.jumpmind.symmetric.transport.http;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.zip.GZIPInputStream;

import org.apache.commons.io.IOUtils;
import org.jumpmind.symmetric.transport.IOutgoingWithResponseTransport;

public class HttpOutgoingTransport implements IOutgoingWithResponseTransport {

    URL url;

    BufferedWriter writer;

    BufferedReader reader;

    HttpURLConnection connection;

    public HttpOutgoingTransport(URL url) {
        this.url = url;
    }

    public void close() throws IOException {
        closeWriter();
        closeReader();
        if (connection != null) {
            connection.disconnect();
            connection = null;
        }
    }

    private void closeReader() throws IOException {
        if (reader != null) {
            IOUtils.closeQuietly(reader);
            reader = null;
        }
    }

    private void closeWriter() throws IOException {
        if (writer != null) {
            writer.flush();
            IOUtils.closeQuietly(writer);
            writer = null;
        }
    }

    public BufferedWriter open() throws IOException {
        connection = (HttpURLConnection) url.openConnection();
        connection.setDoInput(true);
        connection.setDoOutput(true);
        connection.setUseCaches(false);
        connection.setRequestMethod("PUT");
        connection.setRequestProperty("accept-encoding", "gzip");
        OutputStream out = connection.getOutputStream();
        OutputStreamWriter wout = new OutputStreamWriter(out, "UTF-8");
        writer = new BufferedWriter(wout);
        return writer;
    }

    public BufferedReader readResponse() throws IOException {
        closeWriter();
        InputStream in = new GZIPInputStream(connection.getInputStream());
        reader = new BufferedReader(new InputStreamReader(in));
        return reader;
    }

    public boolean isOpen() {
        return connection != null;
    }

}
