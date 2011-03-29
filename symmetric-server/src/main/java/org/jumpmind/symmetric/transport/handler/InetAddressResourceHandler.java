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
 * under the License.  */


package org.jumpmind.symmetric.transport.handler;

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.symmetric.SymmetricException;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.common.logging.ILog;
import org.jumpmind.symmetric.common.logging.LogFactory;
import org.jumpmind.symmetric.security.inet.IInetAddressAuthorizer;
import org.jumpmind.symmetric.security.inet.IInetAddressAuthorizerCompiler;
import org.jumpmind.symmetric.security.inet.IRawInetAddressAuthorizer;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.transport.handler.AbstractTransportResourceHandler;

/**
 * 2
 *
 * 
 */
public class InetAddressResourceHandler extends AbstractTransportResourceHandler implements IInetAddressAuthorizer {
    private static final ILog log = LogFactory.getLog(InetAddressResourceHandler.class);

    public static final String FILTER_DELIMITER = ",";

    private List<IRawInetAddressAuthorizer> filters;

    private IInetAddressAuthorizerCompiler addressCompiler;

    private IParameterService parameterService;

    private boolean isMulicastAllowed;

    /**
     * 
     */
    public void clearFilters() {
        filters = null;
    }

    /**
     * @param filterString
     * @throws UnknownHostException
     */
    public synchronized void setAddressFilters(final String filterString) throws UnknownHostException {
        String[] filtersTokens = null;
        if (StringUtils.isBlank(filterString)) {
            filters = Collections.emptyList();
            log.warn("AddressFiltersMissing");
        } else {
            filtersTokens = filterString.split(FILTER_DELIMITER);
            filters = addressCompiler.compile(filtersTokens);
        }
    }

    /**
     * @param sourceAddress
     * @return
     * @throws UnknownHostException
     */
    public boolean isAuthorized(final String sourceAddress) {
        try {
            final InetAddress inetAddress = InetAddress.getByName(sourceAddress);
            return isAuthorized(inetAddress);
        } catch (final UnknownHostException uhe) {
            // TODO if we don't have a valid host/ip, should we just return
            // 'false'?
            throw new IllegalArgumentException(uhe.getMessage(), uhe);
        }

    }

    /**
     * @param checkAddress
     * @return
     */
    public boolean isAuthorized(final InetAddress checkAddress) {
        if (filters == null) {
            final String filterString = parameterService.getString(ParameterConstants.IP_FILTERS);
            log.debug("AddressFilterStringExtracted", filterString);
            try {
                setAddressFilters(filterString);
            } catch (final UnknownHostException e) {
                throw new SymmetricException("AddressFilterStringExtractingFailed");
            }
        }
        final boolean isMulticast = isMulticastAddress(checkAddress);

        if (isMulticast && !isMulicastAllowed) {
            log.info("AddressMultiCastDisallowed", checkAddress.toString());

            return false;
        }

        final byte[] addressBytes = checkAddress.getAddress();
        for (final IRawInetAddressAuthorizer filter : filters) {
            if (filter.isAuthorized(addressBytes)) {
                return true;
            }
        }
        log.info("AddressDenied", checkAddress.toString());
        return false;
    }

    /**
     * @return the isMulicastAllowed.
     */
    public boolean isMulicastAllowed() {
        return isMulicastAllowed;
    }

    /**
     * @param isMulicastAllowed
     *            the isMulicastAllowed to set
     */
    public void setMulicastAllowed(final boolean isMulicastAllowed) {
        this.isMulicastAllowed = isMulicastAllowed;
    }

    /**
     * @param addressCompiler
     *            the addressCompiler to set
     */
    public void setAddressCompiler(final IInetAddressAuthorizerCompiler addressCompiler) {
        this.addressCompiler = addressCompiler;
    }

    /**
     * @param parameterService
     *            the parameterService to set
     */
    public void setParameterService(final IParameterService parameterService) {
        this.parameterService = parameterService;
    }

    /**
     * @param checkAddress
     * @return
     */
    private boolean isMulticastAddress(final InetAddress checkAddress) {
        // Have to cast as the address type as the default
        // InetAddress.isMulticastAddress() always returns 'false'
        if (checkAddress instanceof Inet4Address) {
            final Inet4Address ip4Addr = (Inet4Address) checkAddress;
            return ip4Addr.isMulticastAddress();
        } else if (checkAddress instanceof Inet6Address) {
            final Inet6Address ip6Addr = (Inet6Address) checkAddress;
            return ip6Addr.isMulticastAddress();
        }
        return checkAddress.isMulticastAddress();
    }

}