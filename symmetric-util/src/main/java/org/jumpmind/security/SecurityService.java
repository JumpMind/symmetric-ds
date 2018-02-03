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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.security.spec.AlgorithmParameterSpec;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.DESedeKeySpec;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.PBEParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.NotImplementedException;
import org.jumpmind.exception.IoException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @see ISecurityService
 */
public class SecurityService implements ISecurityService {

    protected Logger log = LoggerFactory.getLogger(getClass());

    protected SecretKey secretKey;

    protected SecurityService() {
    }

    public void init() {
    }
    
    @Override
    public KeyStore getTrustStore() {
        try {
            String keyStoreType = System.getProperty(SecurityConstants.SYSPROP_KEYSTORE_TYPE,
                    SecurityConstants.KEYSTORE_TYPE);
            KeyStore ks = KeyStore.getInstance(keyStoreType);
            FileInputStream is = new FileInputStream(
                    System.getProperty(SecurityConstants.SYSPROP_TRUSTSTORE));
            String password = unobfuscateIfNeeded(SecurityConstants.SYSPROP_TRUSTSTORE_PASSWORD);
            ks.load(is, password != null ? password.toCharArray() : null);
            is.close();
            return ks;
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    @Override
    public KeyStore getKeyStore() {
        try {
            String keyStoreType = System.getProperty(SecurityConstants.SYSPROP_KEYSTORE_TYPE,
                    SecurityConstants.KEYSTORE_TYPE);
            KeyStore ks = KeyStore.getInstance(keyStoreType);
            FileInputStream is = new FileInputStream(
                    System.getProperty(SecurityConstants.SYSPROP_KEYSTORE));
            ks.load(is, getKeyStorePassword().toCharArray());
            is.close();
            return ks;
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    public void installDefaultSslCert(String host) {
        throw new NotImplementedException();
    }

    protected void checkThatKeystoreFileExists() {
        String keyStoreLocation = System.getProperty(SecurityConstants.SYSPROP_KEYSTORE);
        if (keyStoreLocation == null) {
            throw new RuntimeException("System property '" + SecurityConstants.SYSPROP_KEYSTORE + "' is not defined.");
        }
        if (!new File(keyStoreLocation).exists()) {
            throw new IoException(
                    "Could not find the keystore file.  We expected it to exist here: "
                            + keyStoreLocation);
        }
    }

    public String encrypt(String plainText) {
        try {
            checkThatKeystoreFileExists();
            byte[] bytes = plainText.getBytes(SecurityConstants.CHARSET);
            byte[] enc = getCipher(Cipher.ENCRYPT_MODE).doFinal(bytes);
            return new String(Base64.encodeBase64(enc), SecurityConstants.CHARSET);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public String decrypt(String encText) {
        try {
            checkThatKeystoreFileExists();
            byte[] dec = Base64.decodeBase64(encText.getBytes());
            byte[] bytes = getCipher(Cipher.DECRYPT_MODE).doFinal(dec);
            return new String(bytes, SecurityConstants.CHARSET);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public String obfuscate(String plainText) {
        return new String(Base64.encodeBase64(rot13(plainText).getBytes()));
    }

    public String unobfuscate(String obfText) {
        return new String(rot13(new String(Base64.decodeBase64(obfText.getBytes()))));
    }

    private String unobfuscateIfNeeded(String systemPropertyName) {
        String value = System.getProperty(systemPropertyName);
        if (value != null && value.startsWith(SecurityConstants.PREFIX_OBF)) {
            value = unobfuscate(value.substring(SecurityConstants.PREFIX_OBF.length()));
            System.setProperty(systemPropertyName, value);
        }
        return value;
    }
    
    private String rot13(String text) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if ((c >= 'a' && c <= 'm') || (c >= 'A' && c <= 'M')) {
                c += 13;
            } else if ((c >= 'n' && c <= 'z') || (c >= 'N' && c <= 'Z')) {
                c -= 13;
            }
            sb.append(c);
        }
        return sb.toString();
    }

    public Cipher getCipher(int mode) throws Exception {
        if (secretKey == null) {
            secretKey = getSecretKey();
            log.info("Initialized with " + secretKey.getAlgorithm());
        }
        Cipher cipher = Cipher.getInstance(secretKey.getAlgorithm());
        initializeCipher(cipher, mode);
        log.debug("Using {} algorithm provided by {}.", cipher.getAlgorithm(), cipher.getProvider()
                .getName());
        return cipher;
    }

    protected void initializeCipher(Cipher cipher, int mode) throws Exception {
        AlgorithmParameterSpec paramSpec = Cipher.getMaxAllowedParameterSpec(cipher.getAlgorithm());

        if (paramSpec instanceof PBEParameterSpec || cipher.getAlgorithm().startsWith("PBE")) {
            paramSpec = new PBEParameterSpec(SecurityConstants.SALT,
                    SecurityConstants.ITERATION_COUNT);
            cipher.init(mode, secretKey, paramSpec);
        } else if (paramSpec instanceof IvParameterSpec) {
            paramSpec = new IvParameterSpec(SecurityConstants.SALT);
            cipher.init(mode, secretKey, paramSpec);
        } else {
            cipher.init(mode, secretKey);
        }
    }

    protected String getKeyStorePassword() {
        String password = unobfuscateIfNeeded(SecurityConstants.SYSPROP_KEYSTORE_PASSWORD);
        password = (password != null) ? password : SecurityConstants.KEYSTORE_PASSWORD;
        return password;
    }

    protected SecretKey getSecretKey() throws Exception {
        String password = getKeyStorePassword();
        KeyStore.ProtectionParameter param = new KeyStore.PasswordProtection(password.toCharArray());
        KeyStore ks = getKeyStore();
        KeyStore.SecretKeyEntry entry = (KeyStore.SecretKeyEntry) ks.getEntry(
                SecurityConstants.ALIAS_SYM_SECRET_KEY, param);
        if (entry == null) {
            log.debug("Generating secret key");
            entry = new KeyStore.SecretKeyEntry(getDefaultSecretKey());
            log.info("Generated secret key using " + entry.getSecretKey().getAlgorithm());
            ks.setEntry(SecurityConstants.ALIAS_SYM_SECRET_KEY, entry, param);
            saveKeyStore(ks, password);
        } else {
            log.debug("Retrieving secret key");
        }
        return entry.getSecretKey();
    }

    public String nextSecureHexString(int len) {
        if (len <= 0) {
            throw new IllegalArgumentException("length must be positive");
        }

        SecureRandom random = new SecureRandom();
        int maxInt = SecurityConstants.PASSWORD_CHARS.length();
        char[] password = new char[len];
 
        for (int i = 0; i < len; i++) {
            password[i] = SecurityConstants.PASSWORD_CHARS.charAt(random.nextInt(maxInt));
        }

        return new String(password);
    }

    protected SecretKey getDefaultSecretKey() throws Exception {
        SecureRandom random = new SecureRandom();
        byte bytes[] = null; 
        
        try {
			bytes = new byte[SecurityConstants.BITSIZES[0]];
			random.nextBytes(bytes);
        	secretKey = new SecretKeySpec(bytes, SecurityConstants.KEYSPECS[0]);
            initializeCipher(Cipher.getInstance(SecurityConstants.CIPHERS[0]), Cipher.ENCRYPT_MODE);
        } catch (Exception e) {
        	try {
				bytes = new byte[SecurityConstants.BITSIZES[1]];
				random.nextBytes(bytes);	
				SecretKeyFactory kf = SecretKeyFactory.getInstance(SecurityConstants.KEYSPECS[1]);
				secretKey = kf.generateSecret(new DESedeKeySpec(bytes));
				initializeCipher(Cipher.getInstance(SecurityConstants.CIPHERS[1]), Cipher.ENCRYPT_MODE);
        	} catch (Exception ee) {
				bytes = new byte[SecurityConstants.BITSIZES[2]];
				random.nextBytes(bytes);		
				secretKey = new SecretKeySpec(bytes, SecurityConstants.KEYSPECS[2]);
				initializeCipher(Cipher.getInstance(SecurityConstants.CIPHERS[2]), Cipher.ENCRYPT_MODE);
        	}
        }
 
        return secretKey;
    }

    protected void saveKeyStore(KeyStore ks, String password) throws Exception {
        FileOutputStream os = new FileOutputStream(
                System.getProperty(SecurityConstants.SYSPROP_KEYSTORE));
        ks.store(os, password.toCharArray());
        os.close();
    }

}