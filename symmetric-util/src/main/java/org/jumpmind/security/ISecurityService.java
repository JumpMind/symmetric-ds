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

/**
 * Pluggable Service API that is responsible for encrypting and decrypting data.
 */
public interface ISecurityService {

    public void init();
    
    public void installDefaultSslCert(String host);
    
    public String nextSecureHexString(int len);

    public String encrypt(String plainText);
    
    public String decrypt(String encText);
    
    public String obfuscate(String plainText);
    
    public String unobfuscate(String obfText);
    
    public KeyStore getKeyStore();
    
    public KeyStore getTrustStore();

}