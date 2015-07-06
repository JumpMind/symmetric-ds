/**
 * Licensed to JumpMind Inc under one or more contributor
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU General Public License, version 3.0 (GPLv3)
 * (the "License"); you may not use this file except in compliance
 * with the License.
 *
 * You should have received a copy of the GNU General Public License,
 * version 3.0 (GPLv3) along with this library; if not, see
 * <http://www.gnu.org/licenses/>.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.jumpmind.symmetric.util;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.exception.IoException;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.service.RegistrationRequiredException;
import org.jumpmind.symmetric.transport.AuthenticationException;
import org.jumpmind.symmetric.transport.ChannelDisabledException;
import org.jumpmind.symmetric.transport.ConnectionRejectedException;
import org.jumpmind.symmetric.transport.ServiceUnavailableException;
import org.jumpmind.symmetric.transport.SyncDisabledException;
import org.jumpmind.symmetric.web.WebConstants;

final public class SymmetricUtils {
    
    private SymmetricUtils() {
    }
    
    public static String quote(ISymmetricDialect symmetricDialect, String name) {
        String quote = symmetricDialect.getPlatform().getDatabaseInfo().getDelimiterToken();
        if (StringUtils.isNotBlank(quote)) {
            return quote + name + quote;
        } else {
            return name;
        }
    }
    
    public static void analyzeResponseCode(int code) {
        if (WebConstants.SC_SERVICE_BUSY == code) {
            throw new ConnectionRejectedException();
        } else if (WebConstants.SC_SERVICE_UNAVAILABLE == code) {
            throw new ServiceUnavailableException();
        } else if (WebConstants.SC_FORBIDDEN == code) {
            throw new AuthenticationException();
        } else if (WebConstants.SYNC_DISABLED == code) {
            throw new SyncDisabledException();
        } else if (WebConstants.REGISTRATION_REQUIRED == code) {
            throw new RegistrationRequiredException();
        } else if (WebConstants.SC_CHANNEL_DISABLED == code) {
            throw new ChannelDisabledException();
        } else if (200 != code) {
            throw new IoException("Received an unexpected response code of " + code + " from the server");
        }
    }
    
}
