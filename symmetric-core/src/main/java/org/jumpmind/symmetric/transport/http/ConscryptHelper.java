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
package org.jumpmind.symmetric.transport.http;

import java.security.Provider;
import java.security.Security;

import org.conscrypt.Conscrypt;

public class ConscryptHelper {
    protected final static String PROVIDER_NAME = "Conscrypt";

    public void checkProviderInstalled() {
        if (Security.getProvider(PROVIDER_NAME) == null) {
            Security.insertProviderAt(Conscrypt.newProvider(), 1);
        } else {
            Provider[] providers = Security.getProviders();
            if (providers.length > 0 && !providers[0].getName().equals(PROVIDER_NAME)) {
                Security.removeProvider(PROVIDER_NAME);
                Security.insertProviderAt(Conscrypt.newProvider(), 1);
            }
        }
    }
}
