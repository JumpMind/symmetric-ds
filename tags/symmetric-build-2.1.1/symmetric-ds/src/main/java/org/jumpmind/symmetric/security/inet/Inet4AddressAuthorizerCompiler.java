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


package org.jumpmind.symmetric.security.inet;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.jumpmind.symmetric.common.logging.ILog;
import org.jumpmind.symmetric.common.logging.LogFactory;

/**
 * Filter compiler for IPv4 addresses.
 * 
 * 2
 *
 * 
 */
public class Inet4AddressAuthorizerCompiler extends AbstractInetAddressAuthorizerCompiler {
    private static final ILog log = LogFactory.getLog(Inet4AddressAuthorizerCompiler.class);

    public static final String IPv4_OCTET_SEPARATOR = ".";

    public static final String BROADCAST_OCTET = "255";

    public static final int NUM_IPv4_OCTETS = 4;

    public static final short SHORT_MASK = 0x00FF;

    public static final byte ANY = (byte) 0xFF;

    /**
     * Used for comparison of a 'range' of IPv4 addresses. Specifically, the
     * address space between 2 IPv4 addresses, inclusive of the bounds (highest
     * and lowest possible) addresses themselves.
     * 
     * 
     */
    static class RawInet4AddressRangeAuthorizer implements IRawInetAddressAuthorizer {
        private final short[] startAddress;

        private final short[] endAddress;

        RawInet4AddressRangeAuthorizer(final short[] startAddress, final short[] endAddress) {
            super();
            if ((startAddress.length != NUM_IPv4_OCTETS) || (endAddress.length != NUM_IPv4_OCTETS)) {
                throw new IllegalArgumentException("Invalid number of octets in IPv4 address filter");
            }
            this.startAddress = startAddress;
            this.endAddress = endAddress;
        }

        public boolean isAuthorized(final byte[] addrBytes) {
            final short[] addrAsShorts = convertAddressBytesToShort(addrBytes);
            for (int i = 0; i < addrAsShorts.length; i++) {
                // if we don't have an all inclusive octet at the start (255)
                // offset and the octet
                // does not fall within the bounds, it's nix'd
                if ((startAddress[i] != SHORT_MASK)
                        && ((addrAsShorts[i] < startAddress[i]) || (addrAsShorts[i] > endAddress[i]))) {
                    return false;
                }
            }
            return true;
        }
    }

    /**
     * Used for comparison of addresses to a CIDR (Classless Inter-Domain
     * Routing) address block (i.e. '10.5.5.32/27')
     * 
     * 
     */
    static class RawInet4AddressCidrAuthorizer implements IRawInetAddressAuthorizer {
        private final int checkAddress;

        private int cidrMask = 0x80000000;

        private final byte significantBits;

        RawInet4AddressCidrAuthorizer(final byte[] address, final byte signifcantBits) {
            super();
            // Make sure the CIDR notation is valid (0-32 excluding 31)
            if ((signifcantBits < 0) || (signifcantBits > 32)) {
                throw new IllegalArgumentException(String.format("Invalid CIDR Notation '/%s'. Values must be 0-32.",
                        signifcantBits));
            }

            cidrMask = cidrMask >> (signifcantBits - 1);
            this.checkAddress = bytesToCidrInt(address);
            this.significantBits = signifcantBits;
        }

        public boolean isAuthorized(final byte[] addrBytes) {
            // This means that all addrs are allowed (i.e. CIDR notation '<ip
            // address>/0')
            if (significantBits == 0) {
                return true;
            }
            final int convertedAddress = bytesToCidrInt(addrBytes);
            if ((convertedAddress & checkAddress) == convertedAddress) {
                return true;
            }
            return false;
        }

        private int bytesToCidrInt(final byte[] address) {
            if ((address == null) || (address.length != NUM_IPv4_OCTETS)) {
                return 0;
            }
            int addressAsInt = 0;
            addressAsInt = address[3] & 0xFF;
            addressAsInt |= ((address[2] << 8) & 0xFF00);
            addressAsInt |= ((address[1] << 16) & 0xFF0000);
            addressAsInt |= ((address[0] << 24) & 0xFF000000);
            return addressAsInt & cidrMask;
        }
    }

    /**
     * Used for comparison to a static IP address (which may be wildcarded to a
     * broadcast). So, static IP addresses such as <code>10.5.5.32</code> and
     * <code>10.5.5.*</code> are handled by this authorizer.
     * 
     * 
     */
    static class RawInet4AddressAuthorizer implements IRawInetAddressAuthorizer {
        private final byte[] checkAddress;

        RawInet4AddressAuthorizer(final byte[] address) {
            super();
            this.checkAddress = address;
        }

        public boolean isAuthorized(final byte[] addrBytes) {
            for (int i = 0; i < addrBytes.length; i++) {
                if ((checkAddress[i] != ANY) && (addrBytes[i] != checkAddress[i])) {
                    return false;
                }
            }
            return true;
        }
    }

    /**
     * Compiles for IPv4 specific address filter strings.
     * 
     * @param filter
     * @return
     * @throws UnknownHostException
     */
    @Override
    protected IRawInetAddressAuthorizer compileForIpVersion(String filter) throws UnknownHostException {
        filter = filter.trim();

        log.debug("FilterStringIPv4Compiling", filter);

        filter = replaceSymbols(filter);

        final String[] octets = filter.split('\\' + IPv4_OCTET_SEPARATOR);
        if (octets.length != NUM_IPv4_OCTETS) {
            throw new IllegalArgumentException(String.format(
                    "Invalid IPv4 filter. Must have 4 octects separated by: '%s'. Provided: %s Length: %s",
                    IPv4_OCTET_SEPARATOR, filter, octets.length));
        }
        log.debug("FilterRangeValuesChecking", filter);

        if (filter.contains(CIDR_TOKEN)) {
            return compileCidrAuthorizer(filter);
        } else if (filter.contains(RANGE_TOKEN)) {
            return compileRangeAuthorizer(octets);
        } else {
            // Both static and wild-carded addresses apply here
            final Inet4Address addr = (Inet4Address) InetAddress.getByName(filter);
            return new RawInet4AddressAuthorizer(addr.getAddress());
        }
    }

    /**
     * 
     */
    @Override
    protected String getAddressSeparator() {
        return IPv4_OCTET_SEPARATOR;
    }

    /**
     * 
     */
    @Override
    protected String getBroadcastString() {
        return BROADCAST_OCTET;
    }

    /**
     * Mechanism to pull a <code>short</code> from the provided textual octet.
     * Use <code>short</code> values as we have to do comparisons and
     * <code>byte</code>s are signed (and we'd have to add 254 prior to
     * comparison, blah, blah, blah)
     * 
     * @param octect
     * @return
     */
    protected short getOctetFromString(final String octect) {
        final short octetVal = Short.parseShort(octect);

        if ((octetVal > SHORT_MASK) || (octetVal < 0)) {
            throw new IllegalArgumentException("Invalid IPv4 octect: " + octetVal);
        }
        return octetVal;
    }

    /**
     * Utility method to convert between the actual <code>byte[]</code> address
     * representation and a <code>short[]</code> used to perform the address and
     * range comparison.
     * 
     * @param bytes
     * @return
     */
    public static short[] convertAddressBytesToShort(final byte[] bytes) {
        final short[] retVal = new short[bytes.length];
        for (int i = 0; i < bytes.length; i++) {
            retVal[i] = (short) ((SHORT_MASK) & (bytes[i]));
        }
        return retVal;

    }

    /**
     * Utility method to convert between the <code>short[]</code> used for
     * comparison and a <code>byte[]</code> for actual address representation.
     * 
     * @param shorts
     * @return
     */
    public static byte[] convertShortToAddressBytes(final short[] shorts) {
        final byte[] retVal = new byte[shorts.length];
        for (int i = 0; i < shorts.length; i++) {
            retVal[i] = (byte) shorts[i];
        }
        return retVal;
    }

    /**
     * @param octets
     * @return
     * @throws UnknownHostException
     */
    private RawInet4AddressRangeAuthorizer compileRangeAuthorizer(final String[] octets) throws UnknownHostException {
        final short[] startRange = new short[NUM_IPv4_OCTETS];
        final short[] endRange = new short[NUM_IPv4_OCTETS];
        for (int i = 0; i < octets.length; i++) {
            if (octets[i].contains(RANGE_TOKEN)) {
                final String[] range = octets[i].split(RANGE_TOKEN);
                if (range.length != 2) {
                    throw new IllegalArgumentException("Illegal range pattern for filter address octet. Provided: "
                            + octets[i]);
                }

                final short upperBounds = getOctetFromString(range[0]);
                final short lowerBounds = getOctetFromString(range[1]);
                if (upperBounds < lowerBounds) {
                    throw new IllegalArgumentException("Byte Range must be specificed as '<higher bounds>"
                            + RANGE_TOKEN + "<lower bounds>'. Provided: " + octets[i]);
                }
                startRange[i] = lowerBounds;
                endRange[i] = upperBounds;
            } else {
                final short singleVal = getOctetFromString(octets[i]);
                startRange[i] = singleVal;
                endRange[i] = singleVal;
            }
        }
        // Do a validation of the compiled short addr representations
        InetAddress.getByAddress(convertShortToAddressBytes(startRange));
        InetAddress.getByAddress(convertShortToAddressBytes(endRange));

        return new RawInet4AddressRangeAuthorizer(startRange, endRange);
    }

    /**
     * @param octets
     * @return
     * @throws UnknownHostException
     */
    private RawInet4AddressCidrAuthorizer compileCidrAuthorizer(final String filter) throws UnknownHostException {
        if (filter.contains(RANGE_TOKEN) || filter.contains(ANY_TOKEN)) {
            throw new IllegalArgumentException("CIDR formatted filters cannot contain other tokens");
        }
        final String[] cidrNotation = filter.split(CIDR_TOKEN, 2);
        if (cidrNotation.length != 2) {
            throw new IllegalArgumentException("Expected format of CIDR string is '###.###.###.###/##'");
        }
        final InetAddress inetAddr = InetAddress.getByName(cidrNotation[0]);
        final byte significantBits = Byte.parseByte(cidrNotation[1]);
        return new RawInet4AddressCidrAuthorizer(inetAddr.getAddress(), significantBits);
    }
}