/*
 * Licensed to JumpMind Inc under one or more contributor 
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding 
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU Lesser General Public License (the
 * "License"); you may not use this file except in compliance
 * with the License. 
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see           
 * <http://www.gnu.org/licenses/>.
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.  */


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
            if (!StringUtils.isBlank(httpSslVerifiedServerNames)) {
                HttpsURLConnection.setDefaultHostnameVerifier(new HostnameVerifier() {
                    public boolean verify(String s, SSLSession sslsession) {
                        boolean verified = false;
                        if (!StringUtils.isBlank(httpSslVerifiedServerNames)) {
                            if (httpSslVerifiedServerNames.equalsIgnoreCase(Constants.TRANSPORT_HTTPS_VERIFIED_SERVERS_ALL)) {
                                verified = true;
                            } else {
                                String[] names = httpSslVerifiedServerNames.split(",");
                                for (String string : names) {
                                    if (s != null && s.equals(string.trim())) {
                                        verified = true;
                                        break;
                                    }
                                }
                            }
                        }
                        return verified;
                    }
                });
            }
            
            // Allow self signed certs based on the parameter value.
            boolean allowSelfSignedCerts = parameterService.is(ParameterConstants.TRANSPORT_HTTPS_ALLOW_SELF_SIGNED_CERTS, false);
            if (allowSelfSignedCerts) {
                HttpsURLConnection.setDefaultSSLSocketFactory(createSelfSignedSocketFactory());
            }

            return new HttpTransportManager(parameterService);
            
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