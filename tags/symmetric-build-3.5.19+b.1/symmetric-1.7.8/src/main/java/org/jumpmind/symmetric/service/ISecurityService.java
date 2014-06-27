/**
 * Copyright (C) 2005 Big Lots Inc.
 */
package org.jumpmind.symmetric.service;


/**
 * @author elong
 *
 */
public interface ISecurityService {

    public String encrypt(String plainText);
    
    public String decrypt(String encText);

}
