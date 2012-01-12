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
package org.jumpmind.symmetric.transport;

import java.util.HashMap;
import java.util.Map;

import junit.framework.Assert;

import org.jumpmind.symmetric.statistic.MockStatisticManager;
import org.jumpmind.symmetric.transport.ConcurrentConnectionManager.Reservation;
import org.junit.Test;

public class ConcurrentConnectionManagerUnitTest {

    @Test
    public void testRemoveTimedOutReservations() {
        ConcurrentConnectionManager mgr = new ConcurrentConnectionManager(null, new MockStatisticManager());
        Map<String, Reservation> reservations = new HashMap<String, Reservation>();

        String nodeId = "1";
        Reservation current = new ConcurrentConnectionManager.Reservation(nodeId, System.currentTimeMillis()+10000);        
        reservations.put(nodeId, current);
        
        nodeId = "2";
        current = new ConcurrentConnectionManager.Reservation(nodeId,  System.currentTimeMillis()+10000);        
        reservations.put(nodeId, current);

        Assert.assertEquals(2, reservations.size());
        mgr.removeTimedOutReservations(reservations);
        Assert.assertEquals(2, reservations.size());
        current.timeToLiveInMs = System.currentTimeMillis()-10000;
        mgr.removeTimedOutReservations(reservations);
        Assert.assertEquals(1, reservations.size());
    }
}