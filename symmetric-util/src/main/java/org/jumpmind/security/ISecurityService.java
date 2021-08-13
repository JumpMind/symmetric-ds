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
package org.jumpmind.security;

import java.security.KeyStore;
import java.security.KeyStore.PrivateKeyEntry;
import java.security.KeyStore.TrustedCertificateEntry;
import java.security.cert.X509Certificate;

import javax.crypto.Cipher;
import javax.net.ssl.KeyManagerFactory;

/**
 * Pluggable Service API that is responsible for encrypting and decrypting data.
 */
public interface ISecurityService {
    public void init();

    public void installTrustedCert(TrustedCertificateEntry entry);

    public void installDefaultSslCert(String host);

    public void installSslCert(KeyStore.PrivateKeyEntry entry);

    public TrustedCertificateEntry createTrustedCert(byte[] content, String fileType, String alias, String password);

    public PrivateKeyEntry createDefaultSslCert(String host);

    public PrivateKeyEntry createSslCert(byte[] content, String fileType, String alias, String password);

    public X509Certificate getCurrentSslCert();

    public String exportTrustedCert(String alias);

    public String exportCurrentSslCert(boolean includePrivateKey);

    public String nextSecureHexString(int len);

    public String encrypt(String plainText);

    public String decrypt(String encText);

    public String obfuscate(String plainText);

    public String unobfuscate(String obfText);

    public KeyStore getKeyStore();

    public KeyManagerFactory getKeyManagerFactory();

    public KeyStore getTrustStore();

    public void saveTrustStore(KeyStore ks) throws Exception;

    public Cipher getCipher(int cipherMode) throws Exception;
}