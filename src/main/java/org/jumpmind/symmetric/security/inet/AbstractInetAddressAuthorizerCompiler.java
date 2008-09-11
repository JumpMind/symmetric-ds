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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Base class that all {@link IInetAddressAuthorizerCompiler} implementations should extend.
 * 
 * @author dmichels2
 */
public abstract class AbstractInetAddressAuthorizerCompiler implements IInetAddressAuthorizerCompiler
{
    /**
     * Marker token to denote an all inclusive, or wildcarded, IP address piece. This token specifies that all address
     * are valid for this piece of an IP address. Value: {@value}
     */
    public static final String ANY_TOKEN = "*";

    /**
     * Marker token to denote an inclusive range of values in an IP address piece. This token specifies that all address
     * that fall within the range are valid for this piece of an IP address. Value: {@value}
     */
    public static final String RANGE_TOKEN = "-";

    /**
     * CIDR Marker token which separates an address and the number of significant bits used to evaluate authorization.
     * Value: {@value}
     */
    public static final String CIDR_TOKEN = "/";

    protected static final Log logger = LogFactory.getLog(AbstractInetAddressAuthorizerCompiler.class);

    /**
     * Filter string primary compilation entry point.
     * 
     * @param filterStrings
     * @return
     * @throws UnknownHostException
     */
    public List<IRawInetAddressAuthorizer> compile(final String[] filterStrings) throws UnknownHostException
    {
        final List<IRawInetAddressAuthorizer> rawFilters = new ArrayList<IRawInetAddressAuthorizer>();
        for (final String filter : filterStrings)
        {
            logger.info("Compiling filter string: " + filter);
            rawFilters.add(compileForIpVersion(filter));
        }

        return rawFilters;
    }

    /**
     * @param filter
     * @return
     */
    protected String replaceSymbols(String filter)
    {
        if (filter.contains(ANY_TOKEN))
        {
            final String[] octets = filter.split(getAddressSeparator());
            for (final String octet : octets)
            {
                // verify no whitespace
                if (octet.contains(ANY_TOKEN))
                {
                    if (octet.length() > 1)
                    {
                        throw new IllegalArgumentException(String.format(
                            "Illegal wild card. '%s' can be the the only char in the address piece. Provided: '%s'",
                            ANY_TOKEN, octet));
                    }
                }
            }
            filter = filter.replaceAll('\\' + ANY_TOKEN, getBroadcastString());
            logger.info("Replaced wildcard filter to: " + filter);
        }
        return filter;
    }

    /**
     * Method to implement all of the IP version specific filter compilation logic.
     * 
     * @param filter IP filter definition
     * @return
     */
    protected abstract IRawInetAddressAuthorizer compileForIpVersion(String filter) throws UnknownHostException;

    /**
     * @return
     */
    protected abstract String getBroadcastString();

    /**
     * @return
     */
    protected abstract String getAddressSeparator();

}
