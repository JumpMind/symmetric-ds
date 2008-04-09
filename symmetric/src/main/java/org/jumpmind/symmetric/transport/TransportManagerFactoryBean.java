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

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLSession;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.symmetric.config.IRuntimeConfig;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.transport.http.HttpTransportManager;
import org.jumpmind.symmetric.transport.internal.InternalTransportManager;
import org.springframework.beans.factory.FactoryBean;

public class TransportManagerFactoryBean implements FactoryBean {

    private static final String TRANSPORT_HTTP = "http";

    private static final String TRANSPORT_INTERNAL = "internal";

    private IRuntimeConfig runtimeConfiguration;

    private INodeService nodeService;

    private String transport;

    private int httpTimeout;

    private boolean useCompression;

    private String httpSslVerifiedServerNames;

    public Object getObject() throws Exception {
        if (TRANSPORT_HTTP.equalsIgnoreCase(transport)) {
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
            return new HttpTransportManager(runtimeConfiguration, nodeService, httpTimeout, useCompression);
        } else if (TRANSPORT_INTERNAL.equalsIgnoreCase(transport)) {
            return new InternalTransportManager(runtimeConfiguration);
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

    public void setRuntimeConfiguration(IRuntimeConfig runtimeConfiguration) {
        this.runtimeConfiguration = runtimeConfiguration;
    }

    public void setTransport(String transport) {
        this.transport = transport;
    }

    public void setNodeService(INodeService nodeService) {
        this.nodeService = nodeService;
    }

    public void setHttpTimeout(int httpTimeout) {
        this.httpTimeout = httpTimeout;
    }

    public void setUseCompression(boolean useCompression) {
        this.useCompression = useCompression;
    }

    public void setHttpSslVerifiedServerNames(String httpSslVerifiedServerNames) {
        this.httpSslVerifiedServerNames = httpSslVerifiedServerNames;
    }

}
