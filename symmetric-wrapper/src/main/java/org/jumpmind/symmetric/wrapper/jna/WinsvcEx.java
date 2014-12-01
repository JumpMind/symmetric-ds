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
package org.jumpmind.symmetric.wrapper.jna;

import java.util.Arrays;
import java.util.List;

import org.codehaus.mojo.animal_sniffer.IgnoreJRERequirement;

import com.sun.jna.Pointer;
import com.sun.jna.Structure;
import com.sun.jna.platform.win32.Winsvc;

@IgnoreJRERequirement
public interface WinsvcEx extends Winsvc {

    int SERVICE_WIN32_OWN_PROCESS = 0x00000010;
    int SERVICE_AUTO_START = 0x00000002;
    int SERVICE_DEMAND_START = 0x00000003;
    int SERVICE_ERROR_NORMAL = 0x00000001;
    int SERVICE_CONFIG_DESCRIPTION = 1;

    public interface SERVICE_MAIN_FUNCTION extends StdCallCallback {
        void serviceMain(int argc, Pointer argv);
    }

    public static class SERVICE_TABLE_ENTRY extends Structure {
        public String serviceName;
        public SERVICE_MAIN_FUNCTION serviceCallback;

        public SERVICE_TABLE_ENTRY() {
        }
        
        public SERVICE_TABLE_ENTRY(String serviceName, SERVICE_MAIN_FUNCTION serviceCallback) {
            this.serviceName = serviceName;
            this.serviceCallback = serviceCallback;
        }
        
        @Override
        protected List<String> getFieldOrder() {
            return Arrays.asList(new String[] { "serviceName", "serviceCallback" });
        }
    }
}
