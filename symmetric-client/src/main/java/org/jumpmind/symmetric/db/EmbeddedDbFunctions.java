package org.jumpmind.symmetric.db;

import org.apache.commons.codec.binary.Base64;

/**
 * 
 */
public class EmbeddedDbFunctions {

    public static String encodeBase64(byte[] binaryData) {
        return new String(Base64.encodeBase64(binaryData));
    }
}