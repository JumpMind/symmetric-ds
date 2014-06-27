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
import java.util.ArrayList;
import java.util.List;

/**
 * @author dmichels
 */
public class InetAddressAuthorizerCompiler implements IInetAddressAuthorizerCompiler
{
    Inet4AddressAuthorizerCompiler inet4Compiler = new Inet4AddressAuthorizerCompiler();

    Inet6AddressAuthorizerCompiler inet6Compiler = new Inet6AddressAuthorizerCompiler();

    /*
     * (non-Javadoc)
     * 
     * @see org.jumpmind.symmetric.security.inet.IInetAddressFilterCompiler#compile(java.lang.String[])
     */
    public List<IRawInetAddressAuthorizer> compile(final String[] addresses) throws UnknownHostException
    {
        final List<String> inet4Addrs = new ArrayList<String>();
        final List<String> inet6Addrs = new ArrayList<String>();
        for (final String filter : addresses)
        {
            // check IPv4 or IPv6
            if (filter.contains(":"))
            {
                inet6Addrs.add(filter);
            }
            else
            {
                inet4Addrs.add(filter);
            }
        }
        final List<IRawInetAddressAuthorizer> rawFilters = inet4Compiler.compile(inet4Addrs
            .toArray(new String[inet4Addrs.size()]));
        rawFilters.addAll(inet6Compiler.compile(inet6Addrs.toArray(new String[inet6Addrs.size()])));

        return rawFilters;
    }

}
