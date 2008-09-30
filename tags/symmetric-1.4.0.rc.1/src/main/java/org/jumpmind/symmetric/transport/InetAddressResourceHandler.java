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

package org.jumpmind.symmetric.transport;

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.security.inet.IInetAddressAuthorizer;
import org.jumpmind.symmetric.security.inet.IInetAddressAuthorizerCompiler;
import org.jumpmind.symmetric.security.inet.IRawInetAddressAuthorizer;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.transport.handler.AbstractTransportResourceHandler;

/**
 * @author dmichels2
 */
public class InetAddressResourceHandler extends AbstractTransportResourceHandler implements IInetAddressAuthorizer
{
    private static final Log logger = LogFactory.getLog(InetAddressResourceHandler.class);

    public static final String FILTER_DELIMITER = ",";

    private List<IRawInetAddressAuthorizer> filters;

    private IInetAddressAuthorizerCompiler addressCompiler;

    private IParameterService parameterService;

    private boolean isMulicastAllowed;

    /**
     * 
     */
    public void clearFilters()
    {
        filters = null;
    }

    /**
     * @param filterString
     * @throws UnknownHostException
     */
    public synchronized void setAddressFilters(final String filterString) throws UnknownHostException
    {
        String[] filtersTokens = null;
        if (StringUtils.isBlank(filterString))
        {
            filters = Collections.emptyList();
            logger.warn("No address filters were provided to be compiled");
        }
        else
        {
            filtersTokens = filterString.split(FILTER_DELIMITER);
            filters = addressCompiler.compile(filtersTokens);
        }
    }

    /**
     * @param sourceAddress
     * @return
     * @throws UnknownHostException
     */
    public boolean isAuthorized(final String sourceAddress)
    {
        try
        {
            final InetAddress inetAddress = InetAddress.getByName(sourceAddress);
            return isAuthorized(inetAddress);
        }
        catch (final UnknownHostException uhe)
        {
            // TODO if we don't have a valid host/ip, should we just return 'false'?
            throw new IllegalArgumentException(uhe.getMessage(), uhe);
        }

    }

    /**
     * @param checkAddress
     * @return
     */
    public boolean isAuthorized(final InetAddress checkAddress)
    {
        if (filters == null)
        {
            final String filterString = parameterService.getString(ParameterConstants.IP_FILTERS);
            logger.info("Extracted IP filter string from ParameterService as: " + filterString);
            try
            {
                setAddressFilters(filterString);
            }
            catch (final UnknownHostException e)
            {
                throw new IllegalStateException("Could not initialize address filter string");
            }
        }
        final boolean isMulticast = isMulticastAddress(checkAddress);

        if (isMulticast && !isMulicastAllowed)
        {
            logger.info("Allow multicast is 'false'. Denying multicast address: " + checkAddress.toString());

            return false;
        }

        final byte[] addressBytes = checkAddress.getAddress();
        for (final IRawInetAddressAuthorizer filter : filters)
        {
            if (filter.isAuthorized(addressBytes))
            {
                return true;
            }
        }
        if (logger.isInfoEnabled())
        {
            logger.info("Denying Address: " + checkAddress.toString());
        }
        return false;
    }

    /**
     * @return the isMulicastAllowed.
     */
    public boolean isMulicastAllowed()
    {
        return isMulicastAllowed;
    }

    /**
     * @param isMulicastAllowed the isMulicastAllowed to set
     */
    public void setMulicastAllowed(final boolean isMulicastAllowed)
    {
        this.isMulicastAllowed = isMulicastAllowed;
    }

    /**
     * @param addressCompiler the addressCompiler to set
     */
    public void setAddressCompiler(final IInetAddressAuthorizerCompiler addressCompiler)
    {
        this.addressCompiler = addressCompiler;
    }

    /**
     * @param parameterService the parameterService to set
     */
    public void setParameterService(final IParameterService parameterService)
    {
        this.parameterService = parameterService;
    }

    /**
     * @param checkAddress
     * @return
     */
    private boolean isMulticastAddress(final InetAddress checkAddress)
    {
        // Have to cast as the address type as the default
        // InetAddress.isMulticastAddress() always returns 'false'
        if (checkAddress instanceof Inet4Address)
        {
            final Inet4Address ip4Addr = (Inet4Address) checkAddress;
            return ip4Addr.isMulticastAddress();
        }
        else if (checkAddress instanceof Inet6Address)
        {
            final Inet6Address ip6Addr = (Inet6Address) checkAddress;
            return ip6Addr.isMulticastAddress();
        }
        return checkAddress.isMulticastAddress();
    }

}
