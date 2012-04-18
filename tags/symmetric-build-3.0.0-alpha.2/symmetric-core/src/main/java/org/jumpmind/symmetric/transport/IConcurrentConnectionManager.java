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

import java.util.Map;

import org.jumpmind.symmetric.transport.ConcurrentConnectionManager.NodeConnectionStatistics;
import org.jumpmind.symmetric.transport.ConcurrentConnectionManager.Reservation;

public interface IConcurrentConnectionManager {

    public static enum ReservationType {

        /**
         * A hard reservation is one that is expected to be released. It does
         * not have a timeout.
         */
        HARD,

        /**
         * A soft reservation is one that will time out eventually.
         */
        SOFT

    };

    /**
     * @param nodeId
     * @param reservationRequest
     *                if true then hold onto reservation for the time it
     *                typically takes for a node to reconnect after the initial
     *                request. Otherwise, we know that the node has actually
     *                connected for activity.
     * @return true if the connection has been reserved and the node is meant to
     *         proceed with its current operation.
     */
    public boolean reserveConnection(String nodeId, String poolId, ReservationType reservationRequest);

    public boolean releaseConnection(String nodeId, String poolId);

    public int getReservationCount(String poolId);

    public Map<String, Map<String, NodeConnectionStatistics>> getNodeConnectionStatisticsByPoolByNodeId();

    public Map<String, Map<String, Reservation>> getActiveReservationsByNodeByPool();

    public void addToWhitelist(String nodeId);

    public String[] getWhiteList();

    public void removeFromWhiteList(String nodeId);

}