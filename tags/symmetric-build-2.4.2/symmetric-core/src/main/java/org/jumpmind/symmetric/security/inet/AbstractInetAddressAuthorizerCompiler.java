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

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import org.jumpmind.symmetric.common.logging.ILog;
import org.jumpmind.symmetric.common.logging.LogFactory;

/**
 * Base class that all {@link IInetAddressAuthorizerCompiler} implementations
 * should extend.
 *
 */
public abstract class AbstractInetAddressAuthorizerCompiler implements IInetAddressAuthorizerCompiler {
    /**
     * Marker token to denote an all inclusive, or wildcarded, IP address piece.
     * This token specifies that all address are valid for this piece of an IP
     * address. Value: {@value}
     */
    public static final String ANY_TOKEN = "*";

    /**
     * Marker token to denote an inclusive range of values in an IP address
     * piece. This token specifies that all address that fall within the range
     * are valid for this piece of an IP address. Value: {@value}
     */
    public static final String RANGE_TOKEN = "-";

    /**
     * CIDR Marker token which separates an address and the number of
     * significant bits used to evaluate authorization. Value: {@value}
     */
    public static final String CIDR_TOKEN = "/";

    private static final ILog log = LogFactory.getLog(AbstractInetAddressAuthorizerCompiler.class);

    /**
     * Filter string primary compilation entry point.
     * 
     * @param filterStrings
     * @return
     * @throws UnknownHostException
     */
    public List<IRawInetAddressAuthorizer> compile(final String[] filterStrings) throws UnknownHostException {
        final List<IRawInetAddressAuthorizer> rawFilters = new ArrayList<IRawInetAddressAuthorizer>();
        for (final String filter : filterStrings) {
            log.debug("FilterStringCompiling", filter);
            rawFilters.add(compileForIpVersion(filter));
        }

        return rawFilters;
    }

    /**
     * @param filter
     * @return
     */
    protected String replaceSymbols(String filter) {
        if (filter.contains(ANY_TOKEN)) {
            final String[] octets = filter.split(getAddressSeparator());
            for (final String octet : octets) {
                // verify no whitespace
                if (octet.contains(ANY_TOKEN)) {
                    if (octet.length() > 1) {
                        throw new IllegalArgumentException(
                                String
                                        .format(
                                                "Illegal wild card. '%s' can be the the only char in the address piece. Provided: '%s'",
                                                ANY_TOKEN, octet));
                    }
                }
            }
            filter = filter.replaceAll('\\' + ANY_TOKEN, getBroadcastString());
            log.debug("FilterReplaced", filter);
        }
        return filter;
    }

    /**
     * Method to implement all of the IP version specific filter compilation
     * logic.
     * 
     * @param filter
     *            IP filter definition
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