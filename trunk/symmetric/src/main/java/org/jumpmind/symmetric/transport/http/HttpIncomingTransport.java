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

package org.jumpmind.symmetric.transport.http;

import java.io.BufferedReader;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;

import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.IOUtils;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.service.RegistrationNotOpenException;
import org.jumpmind.symmetric.service.RegistrationRequiredException;
import org.jumpmind.symmetric.transport.AuthenticationException;
import org.jumpmind.symmetric.transport.ConnectionRejectedException;
import org.jumpmind.symmetric.transport.IIncomingTransport;
import org.jumpmind.symmetric.transport.SyncDisabledException;
import org.jumpmind.symmetric.web.WebConstants;

public class HttpIncomingTransport implements IIncomingTransport {

    private HttpURLConnection connection;

    private BufferedReader reader;

    private IParameterService parameterService;
    
    public HttpIncomingTransport(HttpURLConnection connection, IParameterService parameterService) {
        this.connection = connection;
        this.parameterService = parameterService;
    }

    public void close() throws IOException {
        IOUtils.closeQuietly(reader);
    }

    public boolean isOpen() {
        return reader != null;
    }

    public BufferedReader open() throws IOException {
    
        boolean manualRedirects = parameterService.is(ParameterConstants.TRANSPORT_HTTP_MANUAL_REDIRECTS_ENABLED, false);
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
        case HttpServletResponse.SC_SERVICE_UNAVAILABLE:
            throw new ConnectionRejectedException();
        case HttpServletResponse.SC_FORBIDDEN:
            throw new AuthenticationException();
        default:
            reader = HttpTransportManager.getReaderFrom(connection);
            return reader;
        }
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
          if (connection instanceof HttpURLConnection)
          {
             ((HttpURLConnection) connection).setInstanceFollowRedirects(false);
          }
         
          redir = false;
          if (connection instanceof HttpURLConnection)
          {
             HttpURLConnection http = (HttpURLConnection) connection;
             int stat = http.getResponseCode();
             if (stat >= 300 && stat <= 307 && stat != 306 &&
                stat != HttpURLConnection.HTTP_NOT_MODIFIED)
             {
                URL base = http.getURL();
                String loc = http.getHeaderField("Location");

                URL target = null;
                if (loc != null)
                {
                   target = new URL(base, loc);
                }
                http.disconnect();
                // Redirection should be allowed only for HTTP and HTTPS
                // and should be limited to 5 redirections at most.
                if (target == null || !(target.getProtocol().equals("http")
                   || target.getProtocol().equals("https"))
                   || redirects >= 5)
                {
                   throw new SecurityException("illegal URL redirect");
                }
                redir = true;
                connection = (HttpURLConnection)target.openConnection();
                redirects++;
             }
          }
       }
       while (redir);
       
       return connection;
    }
}
