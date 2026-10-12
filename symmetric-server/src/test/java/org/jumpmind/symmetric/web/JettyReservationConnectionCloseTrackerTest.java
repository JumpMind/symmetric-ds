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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;

import org.eclipse.jetty.ee11.servlet.ServletChannel;
import org.eclipse.jetty.ee11.servlet.ServletContextRequest;
import org.eclipse.jetty.http.HttpVersion;
import org.eclipse.jetty.io.Connection;
import org.eclipse.jetty.server.ConnectionMetaData;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.transport.ConcurrentConnectionManager;
import org.jumpmind.symmetric.transport.ConcurrentConnectionManager.Reservation;
import org.jumpmind.symmetric.transport.IConcurrentConnectionManager.ReservationType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import jakarta.servlet.http.HttpServletRequest;

class JettyReservationConnectionCloseTrackerTest {
    private static final String NODE_ID = "node1";
    private static final String CHANNEL_ID = "default";
    private static final String PUSH_URI = "/sync/corp-000/push";
    private final JettyReservationConnectionCloseTracker tracker = new JettyReservationConnectionCloseTracker();
    private ConcurrentConnectionManager concurrentConnectionManager;
    private HttpServletRequest request;
    private ConnectionMetaData metaData;
    private Connection connection;
    private Map<String, Object> connectionAttributes;

    @BeforeEach
    void setUp() {
        IParameterService parameterService = mock(IParameterService.class);
        when(parameterService.getInt(ParameterConstants.CONCURRENT_WORKERS)).thenReturn(5);
        when(parameterService.getLong(ParameterConstants.CONCURRENT_RESERVATION_TIMEOUT)).thenReturn(30_000L);
        concurrentConnectionManager = new ConcurrentConnectionManager(parameterService, null, null);
        connection = mock(Connection.class);
        connectionAttributes = new HashMap<>();
        metaData = mock(ConnectionMetaData.class);
        when(metaData.getConnection()).thenReturn(connection);
        when(metaData.getHttpVersion()).thenReturn(HttpVersion.HTTP_1_1);
        when(metaData.isPersistent()).thenReturn(true);
        when(metaData.getAttribute(anyString())).thenAnswer(invocation -> connectionAttributes.get(invocation.getArgument(0)));
        doAnswer(invocation -> connectionAttributes.put(invocation.getArgument(0), invocation.getArgument(1))).when(metaData)
                .setAttribute(anyString(), any());
        ServletContextRequest contextRequest = mock(ServletContextRequest.class);
        when(contextRequest.getConnectionMetaData()).thenReturn(metaData);
        ServletChannel servletChannel = mock(ServletChannel.class);
        when(servletChannel.getServletContextRequest()).thenReturn(contextRequest);
        request = mock(HttpServletRequest.class);
        when(request.getAttribute(ServletChannel.class.getName())).thenReturn(servletChannel);
    }

    @Test
    void testTrack() {
        assertTrue(tracker.track(request, concurrentConnectionManager, NODE_ID, CHANNEL_ID, PUSH_URI,
                reserveSoft(CHANNEL_ID)));
        verify(connection).addEventListener(getListener());
        assertEquals(1, getListener().getTrackedReservationCount());
    }

    @Test
    void testTrack_withRenewedReservation_reusesListener() {
        tracker.track(request, concurrentConnectionManager, NODE_ID, CHANNEL_ID, PUSH_URI, reserveSoft(CHANNEL_ID));
        tracker.track(request, concurrentConnectionManager, NODE_ID, CHANNEL_ID, PUSH_URI, reserveSoft(CHANNEL_ID));
        verify(connection, times(1)).addEventListener(any());
        assertEquals(1, getListener().getTrackedReservationCount());
    }

    @Test
    void testTrack_withNonJettyRequest_returnsFalse() {
        HttpServletRequest otherRequest = mock(HttpServletRequest.class);
        assertFalse(tracker.track(otherRequest, concurrentConnectionManager, NODE_ID, CHANNEL_ID, PUSH_URI,
                reserveSoft(CHANNEL_ID)));
    }

    @Test
    void testTrack_withNonPersistentConnection_returnsFalse() {
        when(metaData.isPersistent()).thenReturn(false);
        assertFalse(tracker.track(request, concurrentConnectionManager, NODE_ID, CHANNEL_ID, PUSH_URI,
                reserveSoft(CHANNEL_ID)));
        verify(connection, never()).addEventListener(any());
    }

    @Test
    void testTrack_withHttp2_returnsFalse() {
        when(metaData.getHttpVersion()).thenReturn(HttpVersion.HTTP_2);
        assertFalse(tracker.track(request, concurrentConnectionManager, NODE_ID, CHANNEL_ID, PUSH_URI,
                reserveSoft(CHANNEL_ID)));
        verify(connection, never()).addEventListener(any());
    }

    @Test
    void testTrack_withNullReservation_returnsFalse() {
        assertFalse(tracker.track(request, concurrentConnectionManager, NODE_ID, CHANNEL_ID, PUSH_URI, null));
        verify(connection, never()).addEventListener(any());
    }

    @Test
    void testOnClosed_releasesSoftReservation() {
        tracker.track(request, concurrentConnectionManager, NODE_ID, CHANNEL_ID, PUSH_URI, reserveSoft(CHANNEL_ID));
        getListener().onClosed(connection);
        assertEquals(0, concurrentConnectionManager.getReservationCount(PUSH_URI));
        assertEquals(0, getListener().getTrackedReservationCount());
    }

    @Test
    void testOnClosed_releasesEachTrackedChannel() {
        tracker.track(request, concurrentConnectionManager, NODE_ID, CHANNEL_ID, PUSH_URI, reserveSoft(CHANNEL_ID));
        tracker.track(request, concurrentConnectionManager, NODE_ID, "reload", PUSH_URI, reserveSoft("reload"));
        getListener().onClosed(connection);
        assertEquals(0, concurrentConnectionManager.getReservationCount(PUSH_URI));
    }

    @Test
    void testOnClosed_afterPushClaimedReservation_keepsReservation() {
        tracker.track(request, concurrentConnectionManager, NODE_ID, CHANNEL_ID, PUSH_URI, reserveSoft(CHANNEL_ID));
        concurrentConnectionManager.reserveConnection(NODE_ID, CHANNEL_ID, PUSH_URI, ReservationType.HARD, true);
        getListener().onClosed(connection);
        assertEquals(ReservationType.HARD, getPoolReservation(CHANNEL_ID).getType());
    }

    @Test
    void testOnClosed_afterRenewalOnAnotherConnection_keepsReservation() {
        tracker.track(request, concurrentConnectionManager, NODE_ID, CHANNEL_ID, PUSH_URI, reserveSoft(CHANNEL_ID));
        Reservation renewedReservation = reserveSoft(CHANNEL_ID);
        getListener().onClosed(connection);
        assertSame(renewedReservation, getPoolReservation(CHANNEL_ID));
        assertEquals(1, concurrentConnectionManager.getReservationCount(PUSH_URI));
    }

    private Reservation reserveSoft(String channelId) {
        return concurrentConnectionManager.reserveConnection(NODE_ID, channelId, PUSH_URI, ReservationType.SOFT, false).reservation();
    }

    private Reservation getPoolReservation(String channelId) {
        return concurrentConnectionManager.getActiveReservationsByNodeByPool().get(PUSH_URI)
                .get(ConcurrentConnectionManager.getReservationIdentifier(NODE_ID, channelId));
    }

    private JettyReservationConnectionCloseTracker.CloseListener getListener() {
        return (JettyReservationConnectionCloseTracker.CloseListener) connectionAttributes.get(JettyReservationConnectionCloseTracker.ATTRIBUTE_NAME);
    }
}
