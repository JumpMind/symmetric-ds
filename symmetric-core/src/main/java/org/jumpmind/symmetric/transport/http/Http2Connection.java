/**
 * Licensed to JumpMind Inc under one or more contributor
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU General Public License, version 3.0 (GPLv3)
 * (the "License"); you may not use this file except in compliance
 * with the License.
 *
 * You should have received a copy of the GNU General Public License,
 * version 3.0 (GPLv3) along with this library; if not, see
 * <http://www.gnu.org/licenses/>.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.jumpmind.symmetric.transport.http;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ProtocolException;
import java.net.URL;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.X509TrustManager;

import org.jumpmind.symmetric.web.WebConstants;
import org.jumpmind.util.CustomizableThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kotlin.Pair;
import okhttp3.CacheControl;
import okhttp3.Headers;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;
import okio.BufferedSink;

public class Http2Connection extends HttpConnection {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    protected static X509TrustManager trustManager;
    
    protected static SSLSocketFactory sslSocketFactory;
    
    protected static HostnameVerifier hostnameVerifier;

    protected ExecutorService executor;

    protected OkHttpClient.Builder clientBuilder;
    
    protected Request.Builder requestBuilder;
    
    protected Response response;
    
    protected String requestMethod;
    
    protected MediaType mediaType;
    
    protected boolean dooutput;
        
    protected OutputStream internalOut;
    
    protected OutputStream externalOut;
    
    protected IOException exception;
    
    public Http2Connection(URL url) throws IOException {
        super(url);
        reset();
    }

    protected void reset() {
        executor = Executors.newSingleThreadExecutor(new CustomizableThreadFactory(Thread.currentThread().getName()));
        clientBuilder = new OkHttpClient.Builder();
        if (sslSocketFactory != null && trustManager != null) {
            clientBuilder = clientBuilder.sslSocketFactory(sslSocketFactory, trustManager);
        }
        clientBuilder.hostnameVerifier(hostnameVerifier).retryOnConnectionFailure(true);
        requestBuilder = new Request.Builder().url(url);
        dooutput = false;
        requestMethod = "GET";
        internalOut = null;
        externalOut = null;
        exception = null;
    }

    @Override
    public synchronized void disconnect() {
        close();
    }

    @Override
    public synchronized void close() {
        closeOutput();
        if (response != null) {
            response.close();
            response = null;
        }
        if (!executor.isTerminated()) {
            executor.shutdownNow();
        }
        reset();
    }

    protected void closeOutput() {
        if (externalOut != null) {
            try {
                externalOut.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            externalOut = null;
            internalOut = null;
        }
    }

    protected synchronized Response getResponse() throws IOException {
        if (response == null) {
            RequestBody requestBody = null;
            if (dooutput) {
                requestBody = new BlockingRequestBody(this);
            }

            Request request = requestBuilder.method(requestMethod, requestBody).build();
            OkHttpClient client = clientBuilder.build();

            if (log.isDebugEnabled()) {
                logHeaders("Request", request.headers());
            }

            try {
                response = client.newCall(request).execute();
                if (log.isDebugEnabled()) {
                    logHeaders("Response", response.headers());
                }
            } catch (IOException e) {
                exception = e;
                throw e;
            }
            if (log.isDebugEnabled()) {
                log.debug("HTTP response: {}", response.code());
            }
        }
        return response;
    }

    protected void logHeaders(String name, Headers headers) {
        StringBuilder sb = new StringBuilder("{");
        Iterator<Pair<String, String>> iter = headers.iterator();
        while (iter.hasNext()) {
            Pair<String, String> header = iter.next();
            if (!header.getFirst().equalsIgnoreCase(WebConstants.HEADER_SESSION_ID) &&
                    !header.getFirst().equalsIgnoreCase(WebConstants.HEADER_SET_SESSION_ID) &&
                    !header.getFirst().equalsIgnoreCase(WebConstants.REG_PASSWORD) &&
                    !header.getFirst().equalsIgnoreCase(WebConstants.HEADER_SECURITY_TOKEN)) {
                if (sb.length() > 1) {
                    sb.append(", ");
                }
                sb.append(header.getFirst()).append("=").append(header.getSecond());
            }
        }
        sb.append("}");
        log.debug(name + " headers: {}", sb.toString());
    }

    @Override
    public int getResponseCode() throws IOException {
        waitForResponse();
        return getResponse().code();
    }

    @Override
    public InputStream getInputStream() throws IOException {
        closeOutput();
        waitForResponse();
        InputStream stream = null;
        Response resp = getResponse();
        if (resp != null) {
        	ResponseBody respBody = response.body();
        	if (respBody != null) {
        		stream = respBody.byteStream();
        	}
        }
        return stream;
    }

    public String getResponseBody() throws IOException {
        waitForResponse();
        String body = null;
        if (response != null) {
        	ResponseBody responseBody = response.body();
        	if (responseBody != null) {
        		body = responseBody.string();
        	}
        }
        return body;
    }

    protected void waitForResponse() throws IOException {
        if (dooutput) {
            if (!executor.isTerminated()) {
                try {
                    log.debug("Waiting for HTTP thread");
                    long hours = 1;
                    while (!executor.awaitTermination(1, TimeUnit.HOURS)) {
                        log.info("Waiting for HTTP thread for {} hours", hours);
                        hours++;
                    }
                    log.debug("Done waiting for HTTP thread");
                } catch (InterruptedException e) {
                    throw new IOException(e);
                }
            }
            if (exception != null) {
                throw new IOException(exception);
            }
        }
    }

    @Override
    public synchronized OutputStream getOutputStream() throws IOException {
        if (externalOut == null) {
            externalOut = new BlockingOutputStream(this);
            Callable<Response> callable = new CallableResponse(this);
            Future<Response> future = executor.submit(callable);
            executor.shutdown();
            try {
                log.debug("Waiting for output stream");
                // wait until internal thread on BlockingRequestBody.writeTo() has the internal output stream
                wait();
            } catch (InterruptedException e) {
                executor.shutdownNow();
                throw new IOException(e);
            }

            if (future != null && internalOut == null) {
                try {
                    future.get();
                } catch (InterruptedException e) {
                    throw new IOException(e);
                } catch (ExecutionException e) {
                    if (e.getCause() != null) {
                        if (e.getCause() instanceof IOException) {
                            throw (IOException) e.getCause();
                        }
                        throw new IOException(e.getCause());
                    }
                    throw new IOException(e);
                }
            }
        }
        return externalOut;
    }

    @Override
    public synchronized String getContentEncoding() {
        return response == null ? null : response.header("Content-Encoding");
    }

    @Override
    public synchronized String getHeaderField(String name) {
        return response == null ? null : response.header(name);
    }

    @Override
    public synchronized Map<String, List<String>> getHeaderFields() {
        return response == null ? new HashMap<String, List<String>>() : response.headers().toMultimap();
    }

    @Override
    public void setConnectTimeout(int timeout) {
        clientBuilder = clientBuilder.connectTimeout(timeout, TimeUnit.MILLISECONDS);
    }

    @Override
    public void setReadTimeout(int timeout) {
        clientBuilder = clientBuilder.readTimeout(timeout, TimeUnit.MILLISECONDS);
    }

    @Override
    public void setInstanceFollowRedirects(boolean followRedirects) {
        clientBuilder = clientBuilder.followRedirects(true).followSslRedirects(true);
    }

    @Override
    public void setRequestMethod(String method) throws ProtocolException {
        requestMethod = method;
    }

    @Override
    public void setDoInput(boolean doinput) {
    }

    @Override
    public void setDoOutput(boolean dooutput) {
        this.dooutput = dooutput;
    }

    @Override
    public void setAllowUserInteraction(boolean allowuserinteraction) {
    }

    @Override
    public void setChunkedStreamingMode(int chunklen) {
    }

    @Override
    public void setUseCaches(boolean usecaches) {
        requestBuilder = requestBuilder.cacheControl(new CacheControl.Builder().noCache().build());
    }

    public void setRequestProperty(String key, String value) {
        detectMediaType(key, value);
        requestBuilder = requestBuilder.header(key, value);
    }

    @Override
    public void addRequestProperty(String key, String value) {
        detectMediaType(key, value);
        requestBuilder = requestBuilder.addHeader(key, value);
    }

    protected void detectMediaType(String key, String value) {
        if (key.equalsIgnoreCase("Content-Type")) {
            mediaType = MediaType.parse(value);
        }
    }

    public static X509TrustManager getTrustManager() {
        return trustManager;
    }

    public static void setTrustManager(X509TrustManager trustManager) {
        Http2Connection.trustManager = trustManager;
    }

    public static SSLSocketFactory getSslSocketFactory() {
        return sslSocketFactory;
    }

    public static void setSslSocketFactory(SSLSocketFactory sslSocketFactory) {
        Http2Connection.sslSocketFactory = sslSocketFactory;
    }

    public static HostnameVerifier getHostnameVerifier() {
        return hostnameVerifier;
    }

    public static void setHostnameVerifier(HostnameVerifier hostnameVerifier) {
        Http2Connection.hostnameVerifier = hostnameVerifier;
    }    

    protected class CallableResponse implements Callable<Response> {

        protected Http2Connection connection;
        
        protected CallableResponse(Http2Connection connection) {
            this.connection = connection;
        }

        @Override
        public Response call() throws Exception {
            try {
                exception = null;
                return getResponse();
            } catch (Throwable t) {
                log.debug("No output stream, caught exception instead", t);
                synchronized (connection) {
                    internalOut = null;
                    // awake calling thread on getOutputStream() so it can throw this exception
                    connection.notifyAll();
                    throw t;
                }
            }
        }
    }

    protected class BlockingRequestBody extends RequestBody {
        
        protected Http2Connection connection;
        
        protected BlockingRequestBody(Http2Connection connection) {
            this.connection = connection;
        }
        
        @Override
        public MediaType contentType() {
            if (mediaType != null) {
                return mediaType;
            } else if (requestMethod.equalsIgnoreCase("PUT")) {
                return null;
            } else if (requestMethod.equalsIgnoreCase("POST")) {
                return MediaType.parse("application/x-www-form-urlencoded");
            } else {
                return MediaType.parse("text/plain");
            }
        }

        @Override
        public void writeTo(BufferedSink sink) throws IOException {
            internalOut = sink.outputStream();
            synchronized (connection) {
                log.debug("Ready with output stream");
                // awake calling thread on getOutputStream() to return external output stream
                connection.notifyAll();
                try {
                    log.debug("Waiting for output stream to close");
                    // wait for calling thread to close the external output stream
                    connection.wait();
                } catch (InterruptedException e) {
                    throw new IOException(e);
                }
            }
        }
    }

    protected class BlockingOutputStream extends OutputStream {
        
        protected Http2Connection connection;
        
        protected boolean closed;
        
        protected BlockingOutputStream(Http2Connection connection) {
            this.connection = connection;
        }

        @Override
        public void write(int b) throws IOException {
            internalOut.write(b);
        }

        @Override
        public void flush() throws IOException {
            internalOut.flush();
        }

        @Override
        public void close() throws IOException {
            if (!closed) {
                synchronized (connection) {
                    log.debug("Closing output stream");
                    // awake internal thread on BlockingRequestBody.writeTo() so HTTP request can finish
                    connection.notifyAll();
                }
                closed = true;
            }
        }
    }

}
