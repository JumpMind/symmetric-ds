/**
 * Licensed to JumpMind Inc under one or more contributor
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU Affero General Public License, version 3.0 (AGPLv3)
 * (the "License"); you may not use this file except in compliance
 * with the License.
 *
 * You should have received a copy of the GNU Affero General Public License,
 * version 3.0 (AGPLv3) along with this library; if not, see
 * <http://www.gnu.org/licenses/>.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.jumpmind.symmetric.web;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.eclipse.jetty.ee11.servlet.ServletContextRequest;
import org.eclipse.jetty.http.HttpVersion;
import org.eclipse.jetty.io.Connection;
import org.eclipse.jetty.server.ConnectionMetaData;
import org.jumpmind.symmetric.transport.ConcurrentConnectionManager;
import org.jumpmind.symmetric.transport.ConcurrentConnectionManager.Reservation;
import org.jumpmind.symmetric.transport.IConcurrentConnectionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.servlet.http.HttpServletRequest;

/**
 * Jetty implementation of {@link IReservationConnectionCloseTracker}. One {@link CloseListener} is attached per connection and kept as a connection attribute,
 * so keep-alive connections reused for many push cycles do not accumulate listeners. This is the only class that depends on Jetty types.
 */
public class JettyReservationConnectionCloseTracker implements IReservationConnectionCloseTracker {
    static final String ATTRIBUTE_NAME = CloseListener.class.getName();

    @Override
    public boolean registerForRelease(HttpServletRequest req, IConcurrentConnectionManager concurrentConnectionManager, String nodeId, String channelId,
            String poolId,
            Reservation reservation) {
        if (reservation == null) {
            return false;
        }
        ServletContextRequest contextRequest;
        try {
            contextRequest = ServletContextRequest.getServletContextRequest(req);
        } catch (IllegalStateException ex) {
            return false;
        }
        if (contextRequest == null) {
            return false;
        }
        ConnectionMetaData metaData = contextRequest.getConnectionMetaData();
        if (metaData == null || !isPersistentHttp1(metaData)) {
            return false;
        }
        CloseListener listener = (CloseListener) metaData.getAttribute(ATTRIBUTE_NAME);
        if (listener == null) {
            listener = new CloseListener();
            metaData.setAttribute(ATTRIBUTE_NAME, listener);
            metaData.getConnection().addEventListener(listener);
        }
        listener.reservationsById.put(poolId + " " + ConcurrentConnectionManager.getReservationIdentifier(nodeId, channelId),
                new TrackedReservation(concurrentConnectionManager, nodeId, channelId, poolId, reservation));
        return true;
    }

    private static boolean isPersistentHttp1(ConnectionMetaData metaData) {
        HttpVersion version = metaData.getHttpVersion();
        return metaData.isPersistent() && (version == HttpVersion.HTTP_1_1 || version == HttpVersion.HTTP_1_0);
    }

    static class CloseListener implements Connection.Listener {
        private static final Logger log = LoggerFactory.getLogger(CloseListener.class);
        private final Map<String, TrackedReservation> reservationsById = new ConcurrentHashMap<>();

        @Override
        public void onClosed(Connection connection) {
            for (TrackedReservation tracked : reservationsById.values()) {
                if (tracked.concurrentConnectionManager().releaseSoftReservation(tracked.nodeId(), tracked.channelId(), tracked.poolId(),
                        tracked.reservation())) {
                    log.info("Released reservation for node={}, pool={}, channel={} because its connection closed before a push was received",
                            tracked.nodeId(), tracked.poolId(), tracked.channelId());
                }
            }
            reservationsById.clear();
        }

        int getTrackedReservationCount() {
            return reservationsById.size();
        }
    }

    private record TrackedReservation(IConcurrentConnectionManager concurrentConnectionManager, String nodeId, String channelId, String poolId,
            Reservation reservation) {
    }
}
