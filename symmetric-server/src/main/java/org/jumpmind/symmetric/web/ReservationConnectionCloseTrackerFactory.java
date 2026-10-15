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

import java.util.concurrent.atomic.AtomicBoolean;

import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.transport.ConcurrentConnectionManager.Reservation;
import org.jumpmind.symmetric.transport.IConcurrentConnectionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.servlet.http.HttpServletRequest;

/**
 * Chooses the {@link IReservationConnectionCloseTracker} for the web server this engine runs in. Only Jetty is supported, so any other web server gets a
 * tracker that leaves reservations to expire.
 */
class ReservationConnectionCloseTrackerFactory {
    private static final Logger log = LoggerFactory.getLogger(ReservationConnectionCloseTrackerFactory.class);
    private static final String[] JETTY_CLASS_NAMES = { "org.eclipse.jetty.ee11.servlet.ServletContextRequest", "org.eclipse.jetty.io.Connection" };

    private ReservationConnectionCloseTrackerFactory() {
    }

    public static IReservationConnectionCloseTracker create() {
        return create(ReservationConnectionCloseTrackerFactory.class.getClassLoader());
    }

    static IReservationConnectionCloseTracker create(ClassLoader classLoader) {
        if (isJettyAvailable(classLoader)) {
            return new JettyReservationConnectionCloseTracker();
        }
        return new UnsupportedReservationConnectionCloseTracker();
    }

    private static boolean isJettyAvailable(ClassLoader classLoader) {
        try {
            for (String className : JETTY_CLASS_NAMES) {
                Class.forName(className, false, classLoader);
            }
            return true;
        } catch (ClassNotFoundException | LinkageError ex) {
            return false;
        }
    }

    private static class UnsupportedReservationConnectionCloseTracker implements IReservationConnectionCloseTracker {
        private final AtomicBoolean isWarned = new AtomicBoolean();

        @Override
        public boolean isMarkedForRelease(HttpServletRequest req, IConcurrentConnectionManager concurrentConnectionManager, String nodeId, String channelId,
                String poolId, Reservation reservation) {
            if (isWarned.compareAndSet(false, true)) {
                log.warn("Ignoring {} because this web server does not support tracking connection close",
                        ParameterConstants.CONCURRENT_RESERVATION_RELEASE_ON_CLOSE);
            }
            return false;
        }
    }
}
