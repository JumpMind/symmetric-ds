/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Eric Long <erilong@users.sourceforge.net>
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
package org.jumpmind.symmetric.common;

public class SecurityConstants {

    public static final String PREFIX_ENC = "enc:";

    public static final String ALGORITHM = "PBEWithMD5AndTripleDES";

    public static final int ITERATION_COUNT = 3;

    public static final String CHARSET = "UTF8";

    public static final String KEYSTORE_PASSWORD = "changeit";
    
    public static final String KEYSTORE_TYPE = "JCEKS";

    public static final byte[] SALT = { (byte) 0x01, (byte) 0x03, (byte) 0x05, (byte) 0x07, (byte) 0xA2,
            (byte) 0xB4, (byte) 0xC6, (byte) 0xD8 };
    
    public static final String ALIAS_SYM_PRIVATE_KEY = "sym";
    
    public static final String ALIAS_SYM_SECRET_KEY = "sym.secret";
    
    public static final String SYSPROP_KEYSTORE = "sym.keystore.file";
    
    public static final String SYSPROP_KEYSTORE_PASSWORD = "javax.net.ssl.keyStorePassword";
    
    public static final String EMBEDDED_WEBSERVER_DEFAULT_ROLE="symmetric";

}
