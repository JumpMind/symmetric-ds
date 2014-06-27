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

/**
 * 
 *
 * 
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