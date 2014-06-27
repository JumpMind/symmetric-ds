/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Dave Michels <dmichels2@users.sourceforge.net>,
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

package org.jumpmind.symmetric.security.inet;

import java.net.UnknownHostException;

/**
 * <b>Unimplemented</b>. Need to add IPv6 support
 * 
 * @author dmichels
 */
public class Inet6AddressAuthorizerCompiler extends AbstractInetAddressAuthorizerCompiler
{
    /**
     * Constant that defines the separator for the pieces of an IPv6 address. These are ':' and '.' (where '.' is in the
     * case of an IPv4 compatible IPv6 address)
     */
    public static final String IPv6_SEPARATOR = ":.";

    public static final String BROADCAST_PIECE = "FFFF";

    public static final int NUM_IPv6_PIECES = 8;

    /**
     * <b>This method currently throws an <code>UnsupportedOperationException</code> as IPv6 has not yet been impl'd</b>
     * 
     * @param filter
     */
    @Override
    public IRawInetAddressAuthorizer compileForIpVersion(final String filter) throws UnknownHostException
    {
        // TODO this needs to be gutted for IPv6 support
        throw new UnsupportedOperationException("IPv6 filters are currently not supported");
    }

    @Override
    protected String getAddressSeparator()
    {
        return IPv6_SEPARATOR;
    }

    @Override
    protected String getBroadcastString()
    {
        return BROADCAST_PIECE;
    }

}
