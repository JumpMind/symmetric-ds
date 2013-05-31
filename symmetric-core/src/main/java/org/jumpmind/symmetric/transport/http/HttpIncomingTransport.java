
package org.jumpmind.symmetric.transport.http;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;

import org.apache.commons.io.IOUtils;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.service.RegistrationNotOpenException;
import org.jumpmind.symmetric.service.RegistrationRequiredException;
import org.jumpmind.symmetric.transport.AuthenticationException;
import org.jumpmind.symmetric.transport.ConnectionRejectedException;
import org.jumpmind.symmetric.transport.IIncomingTransport;
import org.jumpmind.symmetric.transport.SyncDisabledException;
import org.jumpmind.symmetric.transport.TransportUtils;
import org.jumpmind.symmetric.web.WebConstants;

public class HttpIncomingTransport implements IIncomingTransport {

    private HttpURLConnection connection;

    private BufferedReader reader;
    
    private InputStream is;

    private IParameterService parameterService;
    
    private int httpTimeout;
    
    private String redirectionUrl;
    
    public HttpIncomingTransport(HttpURLConnection connection, IParameterService parameterService) {
        this.connection = connection;
        this.parameterService = parameterService;
        this.httpTimeout = parameterService.getInt(ParameterConstants.TRANSPORT_HTTP_TIMEOUT);
    }
    
    public String getUrl() {
        return this.connection.getURL().toExternalForm();
    }

    public void close() {
        if (reader != null) {
            IOUtils.closeQuietly(reader);
            reader = null;
        } 
        
        if (is != null) {
            IOUtils.closeQuietly(is);
            is = null;
        }
    }

    public boolean isOpen() {
        return reader != null;
    }
    
    public String getRedirectionUrl() {
        return redirectionUrl;
    }
    
    public InputStream openStream() throws IOException {
        
        boolean manualRedirects = parameterService.is(ParameterConstants.TRANSPORT_HTTP_MANUAL_REDIRECTS_ENABLED, true);
        if (manualRedirects) {
            connection = this.openConnectionCheckRedirects(connection);
        }
        
        switch (connection.getResponseCode()) {
        case WebConstants.REGISTRATION_NOT_OPEN:
            throw new RegistrationNotOpenException();
        case WebConstants.REGISTRATION_REQUIRED:
            throw new RegistrationRequiredException();
        case WebConstants.SYNC_DISABLED:
            throw new SyncDisabledException();
        case WebConstants.SC_SERVICE_UNAVAILABLE:
            throw new ConnectionRejectedException();
        case WebConstants.SC_FORBIDDEN:
            throw new AuthenticationException();
        default:
            is = HttpTransportManager.getInputStreamFrom(connection);
            return is;
        }
    }
    

    public BufferedReader openReader() throws IOException {
        InputStream is = openStream();
        reader = TransportUtils.toReader(is);
        return reader;
    }
    
    /**
     * This method support redirection from an http connection to an https connection.
     * See {@link http://java.sun.com/j2se/1.4.2/docs/guide/deployment/deployment-guide/upgrade-guide/article-17.html}
     * for more information.
     * 
     * @param connection
     * @return
     * @throws IOException
     */
    private HttpURLConnection openConnectionCheckRedirects(HttpURLConnection connection) throws IOException
    {      
       boolean redir;
       int redirects = 0;
       do
       {
          connection.setInstanceFollowRedirects(false);         
          redir = false;
             int stat = connection.getResponseCode();
             if (stat >= 300 && stat <= 307 && stat != 306 &&
                stat != HttpURLConnection.HTTP_NOT_MODIFIED)
             {
                URL base = connection.getURL();
                redirectionUrl = connection.getHeaderField("Location");

                URL target = null;
                if (redirectionUrl != null)
                {
                   target = new URL(base, redirectionUrl);
                }
                connection.disconnect();
                // Redirection should be allowed only for HTTP and HTTPS
                // and should be limited to 5 redirections at most.
                if (target == null || !(target.getProtocol().equals("http")
                   || target.getProtocol().equals("https"))
                   || redirects >= 5)
                {
                   throw new SecurityException("illegal URL redirect");
                }
                redir = true;
                connection = HttpTransportManager.openConnection(target, getBasicAuthUsername(), getBasicAuthPassword());
                connection.setConnectTimeout(httpTimeout);
                connection.setReadTimeout(httpTimeout);

                redirects++;
             }
       }
       while (redir);
       
       return connection;
    }
    
    protected String getBasicAuthUsername() {
        return parameterService.getString(ParameterConstants.TRANSPORT_HTTP_BASIC_AUTH_USERNAME);
    }

    protected String getBasicAuthPassword() {
        return parameterService.getString(ParameterConstants.TRANSPORT_HTTP_BASIC_AUTH_PASSWORD);
    }
}