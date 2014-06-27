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
 * under the License. 
 */

package org.jumpmind.symmetric.transport;

import java.security.GeneralSecurityException;
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
import org.jumpmind.exception.SecurityException;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.common.ServerConstants;
import org.jumpmind.symmetric.transport.http.HttpTransportManager;
import org.jumpmind.symmetric.transport.http.SelfSignedX509TrustManager;
import org.jumpmind.symmetric.transport.internal.InternalTransportManager;

public class TransportManagerFactory {

    private ISymmetricEngine symmetricEngine;

    public TransportManagerFactory(ISymmetricEngine symmetricEngine) {
        this.symmetricEngine = symmetricEngine;
    }

    public ITransportManager create() {
        try {
            String transport = symmetricEngine.getParameterService().getString(
                    ParameterConstants.TRANSPORT_TYPE);
            if (Constants.PROTOCOL_HTTP.equalsIgnoreCase(transport)) {
                final String httpSslVerifiedServerNames = symmetricEngine.getParameterService()
                        .getString(ServerConstants.HTTPS_VERIFIED_SERVERS);
                if (!StringUtils.isBlank(httpSslVerifiedServerNames)) {
                    HttpsURLConnection.setDefaultHostnameVerifier(new HostnameVerifier() {
                        public boolean verify(String s, SSLSession sslsession) {
                            boolean verified = false;
                            if (!StringUtils.isBlank(httpSslVerifiedServerNames)) {
                                if (httpSslVerifiedServerNames
                                        .equalsIgnoreCase(Constants.TRANSPORT_HTTPS_VERIFIED_SERVERS_ALL)) {
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
                boolean allowSelfSignedCerts = symmetricEngine.getParameterService().is(
                        ServerConstants.HTTPS_ALLOW_SELF_SIGNED_CERTS, false);
                if (allowSelfSignedCerts) {
                    HttpsURLConnection.setDefaultSSLSocketFactory(createSelfSignedSocketFactory());
                }

                return new HttpTransportManager(symmetricEngine);

            } else if (Constants.PROTOCOL_INTERNAL.equalsIgnoreCase(transport)) {
                return new InternalTransportManager(symmetricEngine);
            } else {
                throw new IllegalStateException("An invalid transport type of " + transport
                        + " was specified.");
            }
        } catch (GeneralSecurityException ex) {
            throw new SecurityException(ex);
        }
    }

    /**
     * Create an SSL Socket Factory that accepts self signed certificates.
     * 
     * @return
     * @throws NoSuchAlgorithmException
     * @throws KeyManagementException
     * @throws KeyStoreException
     */
    private static SSLSocketFactory createSelfSignedSocketFactory()
            throws NoSuchAlgorithmException, KeyManagementException, KeyStoreException {
        SSLSocketFactory factory = null;

        SSLContext context = SSLContext.getInstance("TLS");
        context.init(null, new TrustManager[] { new SelfSignedX509TrustManager(null) },
                new SecureRandom());
        factory = context.getSocketFactory();

        return factory;
    }

}