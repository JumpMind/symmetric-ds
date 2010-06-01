/**
 * Copyright (C) 2005 Big Lots Inc.
 */

package org.jumpmind.symmetric.security.inet;

/**
 * Provides lower-level address authorization to allow for raw address bytes to be authorized as opposed to requiring an
 * <code>InetAddress</code> to be created for authorization checks.
 * 
 * @author dmichels
 */
public interface IRawInetAddressAuthorizer
{
    public boolean isAuthorized(byte[] addrBytes);
}
