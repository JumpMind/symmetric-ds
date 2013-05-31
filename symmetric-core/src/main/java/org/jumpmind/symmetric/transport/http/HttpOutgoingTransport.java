package org.jumpmind.symmetric.transport.http;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.io.IOUtils;
import org.jumpmind.exception.IoException;
import org.jumpmind.symmetric.io.IoConstants;
import org.jumpmind.symmetric.model.ChannelMap;
import org.jumpmind.symmetric.service.IConfigurationService;
import org.jumpmind.symmetric.service.RegistrationRequiredException;
import org.jumpmind.symmetric.transport.AuthenticationException;
import org.jumpmind.symmetric.transport.ConnectionRejectedException;
import org.jumpmind.symmetric.transport.IOutgoingWithResponseTransport;
import org.jumpmind.symmetric.transport.SyncDisabledException;
import org.jumpmind.symmetric.web.WebConstants;

public class HttpOutgoingTransport implements IOutgoingWithResponseTransport {

    static final String CRLF = "\r\n";
    
    private String boundary;
    
    private URL url;

    private OutputStream os;

    private BufferedWriter writer;

    private BufferedReader reader;

    private HttpURLConnection connection;

    private int httpTimeout;

    private boolean useCompression;

    private int compressionStrategy;

    private int compressionLevel;

    private String basicAuthUsername;

    private String basicAuthPassword;

    private boolean streamOutputEnabled = false;

    private int streamOutputChunkSize = 30720;

    private boolean fileUpload = false;

    public HttpOutgoingTransport(URL url, int httpTimeout, boolean useCompression,
            int compressionStrategy, int compressionLevel, String basicAuthUsername,
            String basicAuthPassword, boolean streamOutputEnabled, int streamOutputSize,
            boolean fileUpload) {
        this.url = url;
        this.httpTimeout = httpTimeout;
        this.useCompression = useCompression;
        this.compressionLevel = compressionLevel;
        this.compressionStrategy = compressionStrategy;
        this.basicAuthUsername = basicAuthUsername;
        this.basicAuthPassword = basicAuthPassword;
        this.streamOutputChunkSize = streamOutputSize;
        this.streamOutputEnabled = streamOutputEnabled;
        this.fileUpload = fileUpload;
    }

    public void close() {
        closeWriter(true);
        closeOutputStream(true);
        closeReader();
        if (connection != null) {
            connection.disconnect();
            connection = null;
        }
    }

    private void closeReader() {
        if (reader != null) {
            IOUtils.closeQuietly(reader);
            reader = null;
        }
    }
    
    private void closeOutputStream(boolean closeQuietly) {
        if (os != null) {
            try {
                if (fileUpload) {
                    IOUtils.write(CRLF + "--" + boundary + "--" + CRLF, os);                    
                }
                os.flush();
            } catch (IOException ex) {
                throw new IoException(ex);
            } finally {
                if (closeQuietly) {
                    IOUtils.closeQuietly(os);
                } else {
                    try {
                        os.close();
                    } catch (IOException ex) {
                        throw new IoException(ex);
                    }
                }
                os = null;
            }
        }
    }
    
    private void closeWriter(boolean closeQuietly) {
        if (writer != null) {
            try {
                if (fileUpload) {
                    IOUtils.write(CRLF + "--" + boundary + "--" + CRLF, os);
                }
                writer.flush();
            } catch (IOException ex) {
                throw new IoException(ex);
            } finally {
                if (closeQuietly) {
                    IOUtils.closeQuietly(writer);
                } else {
                    try {
                        writer.close();
                    } catch (IOException ex) {
                        throw new IoException(ex);
                    }
                }
                writer = null;
                os = null;
            }
        }
    }

    /**
     * Before streaming data to the remote node, make sure it is ok to. We have
     * found that we can be more efficient on a push by relying on HTTP
     * keep-alive.
     * 
     * @throws IOException
     * @throws {@link ConnectionRejectedException}
     * @throws {@link AuthenticationException}
     */
    private HttpURLConnection requestReservation() {
        try {
            connection = HttpTransportManager.openConnection(url, basicAuthUsername,
                    basicAuthPassword);
            connection.setUseCaches(false);
            connection.setConnectTimeout(httpTimeout);
            connection.setReadTimeout(httpTimeout);
            connection.setRequestMethod("HEAD");

            analyzeResponseCode(connection.getResponseCode());
        } catch (IOException ex) {
            throw new IoException(ex);
        }
        return connection;
    }

    public OutputStream openStream() {
        try {
            connection = HttpTransportManager.openConnection(url, basicAuthUsername,
                    basicAuthPassword);
            if (streamOutputEnabled) {
                connection.setChunkedStreamingMode(streamOutputChunkSize);
            }
            connection.setDoInput(true);
            connection.setDoOutput(true);
            connection.setUseCaches(false);
            connection.setConnectTimeout(httpTimeout);
            connection.setReadTimeout(httpTimeout);

            boundary = Long.toHexString(System.currentTimeMillis());
            if (!fileUpload) {
                connection.setRequestMethod("PUT");
                connection.setRequestProperty("Accept-Encoding", "gzip");
                if (useCompression) {
                    connection.addRequestProperty("Content-Type", "gzip"); // application/x-gzip?
                }
            } else {
                connection.setRequestProperty("Content-Type", "multipart/form-data; boundary="
                        + boundary);
            }

            os = connection.getOutputStream();

            if (!fileUpload && useCompression) {
                os = new GZIPOutputStream(os) {
                    {
                        this.def.setLevel(compressionLevel);
                        this.def.setStrategy(compressionStrategy);
                    }
                };
            }

            if (fileUpload) {
                final String fileName = "file.zip";
                IOUtils.write("--" + boundary + CRLF, os);
                IOUtils.write("Content-Disposition: form-data; name=\"binaryFile\"; filename=\""
                        + fileName + "\"" + CRLF, os);
                IOUtils.write("Content-Type: " + URLConnection.guessContentTypeFromName(fileName)
                        + CRLF, os);
                IOUtils.write("Content-Transfer-Encoding: binary" + CRLF + CRLF, os);
                os.flush();

            }
            return os;
        } catch (IOException ex) {
            throw new IoException(ex);
        }
    }

    public BufferedWriter openWriter() {
        try {
            OutputStreamWriter wout = new OutputStreamWriter(openStream(), IoConstants.ENCODING);
            writer = new BufferedWriter(wout);
            return writer;
        } catch (IOException ex) {
            throw new IoException(ex);
        }
    }

    /**
     * @throws {@link ConnectionRejectedException}
     * @throws {@link AuthenticationException}
     */
    private void analyzeResponseCode(int code) throws IOException {
        if (WebConstants.SC_SERVICE_UNAVAILABLE == code) {
            throw new ConnectionRejectedException();
        } else if (WebConstants.SC_FORBIDDEN == code) {
            throw new AuthenticationException();
        } else if (WebConstants.SYNC_DISABLED == code) {
            throw new SyncDisabledException();
        } else if (WebConstants.REGISTRATION_REQUIRED == code) {
            throw new RegistrationRequiredException();
        }
    }

    public BufferedReader readResponse() throws IOException {
        closeWriter(false);        
        closeOutputStream(false);
        analyzeResponseCode(connection.getResponseCode());
        this.reader = HttpTransportManager.getReaderFrom(connection);
        return this.reader;
    }

    public boolean isOpen() {
        return connection != null;
    }

    public ChannelMap getSuspendIgnoreChannelLists(IConfigurationService configurationService) {

        HttpURLConnection connection = requestReservation();

        // Connection contains remote suspend/ignore channels list if
        // reservation was successful.

        ChannelMap suspendIgnoreChannelsList = new ChannelMap();

        String suspends = connection.getHeaderField(WebConstants.SUSPENDED_CHANNELS);
        String ignores = connection.getHeaderField(WebConstants.IGNORED_CHANNELS);

        suspendIgnoreChannelsList.addSuspendChannels(suspends);
        suspendIgnoreChannelsList.addIgnoreChannels(ignores);

        return suspendIgnoreChannelsList;
    }

}