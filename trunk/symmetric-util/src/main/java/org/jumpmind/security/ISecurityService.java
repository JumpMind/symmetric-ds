package org.jumpmind.security;


/**
 * Pluggable Service API that is responsible for encrypting and decrypting data.
 */
public interface ISecurityService {

    public void init();
    
    public String nextSecureHexString(int len);

    public String encrypt(String plainText);
    
    public String decrypt(String encText);

}