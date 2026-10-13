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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.mockito.Mockito.mock;

import org.jumpmind.symmetric.transport.ConcurrentConnectionManager.Reservation;
import org.jumpmind.symmetric.transport.IConcurrentConnectionManager;
import org.jumpmind.symmetric.transport.IConcurrentConnectionManager.ReservationType;
import org.junit.jupiter.api.Test;

import jakarta.servlet.http.HttpServletRequest;

class ReservationConnectionCloseTrackerFactoryTest {
    @Test
    void testCreate_withJettyOnClasspath() {
        assertInstanceOf(JettyReservationConnectionCloseTracker.class, ReservationConnectionCloseTrackerFactory.create());
    }

    @Test
    void testCreate_withoutJetty_returnsTrackerThatIgnoresConnections() {
        IReservationConnectionCloseTracker tracker = ReservationConnectionCloseTrackerFactory.create(new JettyHidingClassLoader());
        assertNotEquals(JettyReservationConnectionCloseTracker.class, tracker.getClass());
        Reservation reservation = new Reservation("node1", System.currentTimeMillis() + 10_000, ReservationType.SOFT);
        assertFalse(tracker.isMarkedForRelease(mock(HttpServletRequest.class), mock(IConcurrentConnectionManager.class), "node1", "default",
                "/sync/corp-000/push",
                reservation));
    }

    @Test
    void testCreate_withoutJetty_keepsIgnoringOnRepeatedCalls() {
        IReservationConnectionCloseTracker tracker = ReservationConnectionCloseTrackerFactory.create(new JettyHidingClassLoader());
        HttpServletRequest request = mock(HttpServletRequest.class);
        IConcurrentConnectionManager manager = mock(IConcurrentConnectionManager.class);
        Reservation reservation = new Reservation("node1", System.currentTimeMillis() + 10_000, ReservationType.SOFT);
        assertFalse(tracker.isMarkedForRelease(request, manager, "node1", "default", "/sync/corp-000/push", reservation));
        assertFalse(tracker.isMarkedForRelease(request, manager, "node1", "default", "/sync/corp-000/push", reservation));
    }

    private static class JettyHidingClassLoader extends ClassLoader {
        JettyHidingClassLoader() {
            super(ReservationConnectionCloseTrackerFactoryTest.class.getClassLoader());
        }

        @Override
        protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
            if (name.startsWith("org.eclipse.jetty.")) {
                throw new ClassNotFoundException(name);
            }
            return super.loadClass(name, resolve);
        }
    }
}
