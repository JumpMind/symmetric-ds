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

package org.jumpmind.symmetric.transport;

import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.service.IConfigurationService;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.transport.http.HttpTransportManager;
import org.jumpmind.symmetric.transport.http.SelfSignedX509TrustManager;
import org.jumpmind.symmetric.transport.internal.InternalTransportManager;
import org.springframework.beans.factory.FactoryBean;

public class TransportManagerFactoryBean implements FactoryBean<ITransportManager> {

    private IParameterService parameterService;

    private IConfigurationService configurationService;

    public ITransportManager getObject() throws Exception {
        String transport = parameterService.getString(ParameterConstants.TRANSPORT_TYPE);
        if (Constants.PROTOCOL_HTTP.equalsIgnoreCase(transport)) {
            final String httpSslVerifiedServerNames = parameterService
                    .getString(ParameterConstants.TRANSPORT_HTTPS_VERIFIED_SERVERS);
            HttpsURLConnection.setDefaultHostnameVerifier(new HostnameVerifier() {
                public boolean verify(String s, SSLSession sslsession) {
                    if (!StringUtils.isBlank(httpSslVerifiedServerNames)) {
                        String[] names = httpSslVerifiedServerNames.split(",");
                        for (String string : names) {
                            if (s != null && s.equals(string.trim())) {
                                return true;
                            }
                        }
                    }
                    return false;
                }
            });
            
            // Allow self signed certs based on the parameter value.
            boolean allowSelfSignedCerts = parameterService.is(ParameterConstants.TRANSPORT_HTTPS_ALLOW_SELF_SIGNED_CERTS, false);
            if (allowSelfSignedCerts) {
                HttpsURLConnection.setDefaultSSLSocketFactory(createSelfSignedSocketFactory());
            }

            return new HttpTransportManager(parameterService.getInt(ParameterConstants.TRANSPORT_HTTP_TIMEOUT), parameterService.is(ParameterConstants.TRANSPORT_HTTP_USE_COMPRESSION_CLIENT), parameterService.getInt(ParameterConstants.TRANSPORT_HTTP_COMPRESSION_LEVEL), parameterService.getInt(ParameterConstants.TRANSPORT_HTTP_COMPRESSION_STRATEGY));
        } else if (Constants.PROTOCOL_INTERNAL.equalsIgnoreCase(transport)) {
            return new InternalTransportManager(configurationService);
        } else {
            throw new IllegalStateException("An invalid transport type of " + transport + " was specified.");
        }
    }

    public Class<ITransportManager> getObjectType() {
        return ITransportManager.class;
    }

    public boolean isSingleton() {
        return true;
    }

    public void setParameterService(IParameterService parameterService) {
        this.parameterService = parameterService;
    }

    public IConfigurationService getConfigurationService() {
        return configurationService;
    }

    public void setConfigurationService(IConfigurationService configurationService) {
        this.configurationService = configurationService;
    }
    
    /**
     * Create an SSL Socket Factory that accepts self signed certificates.
     * 
     * @return
     * @throws NoSuchAlgorithmException
     * @throws KeyManagementException
     * @throws KeyStoreException
     */
    private static SSLSocketFactory createSelfSignedSocketFactory() throws NoSuchAlgorithmException, KeyManagementException, KeyStoreException {
        SSLSocketFactory factory = null;
        
        SSLContext context = SSLContext.getInstance("TLS");
        context.init(null, 
                     new TrustManager[] {new SelfSignedX509TrustManager(null)}, 
                     new SecureRandom());
        factory = context.getSocketFactory();
        
        return factory;
    }

}
