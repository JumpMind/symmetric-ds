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
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.transport.http.HttpTransportManager;
import org.jumpmind.symmetric.transport.internal.InternalTransportManager;
import org.springframework.beans.factory.FactoryBean;

public class TransportManagerFactoryBean implements FactoryBean {

    private INodeService nodeService;

    private IParameterService parameterService;

    public Object getObject() throws Exception {
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
            return new HttpTransportManager(nodeService, parameterService);
        } else if (Constants.PROTOCOL_INTERNAL.equalsIgnoreCase(transport)) {
            return new InternalTransportManager(nodeService, parameterService);
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

    public void setNodeService(INodeService nodeService) {
        this.nodeService = nodeService;
    }

    public void setParameterService(IParameterService parameterService) {
        this.parameterService = parameterService;
    }

}
