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
package org.jumpmind.symmetric.service.impl;


import static org.junit.Assert.assertTrue;

import org.jumpmind.symmetric.model.NodeCommunication;
import org.jumpmind.symmetric.model.NodeCommunication.CommunicationType;
import org.junit.Test;

public class NodeCommunicationServiceTest {

    @Test
    public void testNodeCommunicationTypeLengths() {
        final int MAX_LENGTH_IN_DB = 10;
        
        for (CommunicationType communicationType : NodeCommunication.CommunicationType.values()) {
            String msg = communicationType.name() + " is too long for DB. " +  communicationType.name().length() + " <= " + MAX_LENGTH_IN_DB;
            assertTrue(msg, communicationType.name().length() <= MAX_LENGTH_IN_DB);
        }
    }
}
