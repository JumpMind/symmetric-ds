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

/**
 * <b>Unimplemented</b>. Need to add IPv6 support
 * 
 * 
 *
 * 
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