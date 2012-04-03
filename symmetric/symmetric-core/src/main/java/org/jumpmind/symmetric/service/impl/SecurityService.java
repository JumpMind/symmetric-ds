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

package org.jumpmind.symmetric.service.impl;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.security.KeyStore;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.spec.AlgorithmParameterSpec;
import java.security.spec.KeySpec;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.PBEParameterSpec;

import org.apache.commons.codec.binary.Base64;
import org.jumpmind.symmetric.common.SecurityConstants;
import org.jumpmind.symmetric.service.ISecurityService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @see ISecurityService
 */
public class SecurityService implements ISecurityService {

    protected Logger log = LoggerFactory.getLogger(getClass());

    protected SecretKey secretKey;
    
    protected SecureRandom secRand;

    public void init() {
    }

    public String encrypt(String plainText) {
        try {
            byte[] bytes = plainText.getBytes(SecurityConstants.CHARSET);
            byte[] enc = getCipher(Cipher.ENCRYPT_MODE).doFinal(bytes);
            return new String(Base64.encodeBase64(enc), SecurityConstants.CHARSET);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public String decrypt(String encText) {
        try {
            byte[] dec = Base64.decodeBase64(encText.getBytes());
            byte[] bytes = getCipher(Cipher.DECRYPT_MODE).doFinal(dec);
            return new String(bytes, SecurityConstants.CHARSET);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected Cipher getCipher(int mode) throws Exception {
        if (secretKey == null) {
            secretKey = getSecretKey();
        }
        Cipher cipher = Cipher.getInstance(secretKey.getAlgorithm());
        initializeCipher(cipher, mode);
        log.info("SecretKeyUsing", cipher.getAlgorithm(), cipher.getProvider().getName());
        return cipher;
    }

    protected void initializeCipher(Cipher cipher, int mode) throws Exception {
        AlgorithmParameterSpec paramSpec = Cipher.getMaxAllowedParameterSpec(cipher.getAlgorithm());
        
        if (paramSpec instanceof PBEParameterSpec) {
            paramSpec = new PBEParameterSpec(SecurityConstants.SALT, SecurityConstants.ITERATION_COUNT);
            cipher.init(mode, secretKey, paramSpec);
        } else if (paramSpec instanceof IvParameterSpec) {
            paramSpec = new IvParameterSpec(SecurityConstants.SALT);
            cipher.init(mode, secretKey, paramSpec);
        } else {
            cipher.init(mode, secretKey, (AlgorithmParameterSpec) null);
        }
    }

    protected SecretKey getSecretKey() throws Exception {
        String password = System.getProperty(SecurityConstants.SYSPROP_KEYSTORE_PASSWORD);
        password = (password != null) ? password : SecurityConstants.KEYSTORE_PASSWORD;
        KeyStore.ProtectionParameter param = new KeyStore.PasswordProtection(password.toCharArray());
        KeyStore ks = getKeyStore(password);
        KeyStore.SecretKeyEntry entry = (KeyStore.SecretKeyEntry) ks.getEntry(SecurityConstants.ALIAS_SYM_SECRET_KEY, param);
        if (entry == null) {
            log.debug("SecretKeyGenerating");
            entry = new KeyStore.SecretKeyEntry(getDefaultSecretKey());
            ks.setEntry(SecurityConstants.ALIAS_SYM_SECRET_KEY, entry, param);
            saveKeyStore(ks, password);
        } else {
            log.debug("SecretKeyRetrieving");
        }
        return entry.getSecretKey();
    }
    
    private SecureRandom getSecRan() {
        if (secRand == null) {
            secRand = new SecureRandom();
            secRand.setSeed(System.currentTimeMillis());
        }
        return secRand;
    }
    
    public String nextSecureHexString(int len) {
        if (len <= 0)
            throw new IllegalArgumentException("length must be positive");
        SecureRandom secRan = getSecRan();
        MessageDigest alg = null;
        try {
            alg = MessageDigest.getInstance("SHA-1");
        } catch (NoSuchAlgorithmException ex) {
            return null;
        }
        alg.reset();
        int numIter = len / 40 + 1;
        StringBuffer outBuffer = new StringBuffer();
        for (int iter = 1; iter < numIter + 1; iter++) {
            byte randomBytes[] = new byte[40];
            secRan.nextBytes(randomBytes);
            alg.update(randomBytes);
            byte hash[] = alg.digest();
            for (int i = 0; i < hash.length; i++) {
                Integer c = new Integer(hash[i]);
                String hex = Integer.toHexString(c.intValue() + 128);
                if (hex.length() == 1)
                    hex = "0" + hex;
                outBuffer.append(hex);
            }

        }

        return outBuffer.toString().substring(0, len);
    }

    protected SecretKey getDefaultSecretKey() throws Exception {
        String keyPassword = nextSecureHexString(16);
        KeySpec keySpec = new PBEKeySpec(keyPassword.toCharArray(), SecurityConstants.SALT, SecurityConstants.ITERATION_COUNT);
        return SecretKeyFactory.getInstance(SecurityConstants.ALGORITHM).generateSecret(keySpec);
    }

    protected KeyStore getKeyStore(String password) throws Exception {
        KeyStore ks = KeyStore.getInstance(SecurityConstants.KEYSTORE_TYPE);
        FileInputStream is = new FileInputStream(System.getProperty(SecurityConstants.SYSPROP_KEYSTORE));
        ks.load(is, password.toCharArray());
        is.close();
        return ks;
    }

    protected void saveKeyStore(KeyStore ks, String password) throws Exception {
        FileOutputStream os = new FileOutputStream(System.getProperty(SecurityConstants.SYSPROP_KEYSTORE));
        ks.store(os, password.toCharArray());
        os.close();
    }

}