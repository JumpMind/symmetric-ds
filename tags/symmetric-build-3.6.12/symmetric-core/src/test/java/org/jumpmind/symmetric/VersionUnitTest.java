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
package org.jumpmind.symmetric;

import static org.junit.Assert.*;
import org.junit.Test;

public class VersionUnitTest {

    @Test
    public void testIsOlderThanVersion() {
        assertTrue(Version.isOlderThanVersion("1.5.1", "1.6.0"));
        assertTrue(Version.isOlderThanVersion("1.3.1", "1.6.0"));
        assertTrue(Version.isOlderThanVersion("1.6.0", "1.6.1"));
        assertFalse(Version.isOlderThanVersion("1.6.0", "1.6.0"));
        assertFalse(Version.isOlderThanVersion("1.6.1", "1.6.0"));
        assertFalse(Version.isOlderThanVersion("2.0.0", "1.6.0"));
    }
    
    @Test
    public void testIsOlderVersion() {
        // test/resources pom.properties contains 1.6.0
        assertTrue(Version.isOlderVersion("1.0.0"));
        assertTrue(Version.isOlderVersion("1.5.0"));
        assertTrue(Version.isOlderVersion("1.5.1"));
        assertTrue(Version.isOlderVersion("1.5.5"));
        assertTrue(Version.isOlderVersion("1.6.0"));
        assertTrue(Version.isOlderVersion("1.6.1"));
        assertFalse(Version.isOlderVersion("3.6.1"));
    }
    
    @Test
    public void testIsMinorOlderVersion() {
        assertTrue(Version.isOlderMinorVersion("1.5", "1.6"));
        assertTrue(Version.isOlderMinorVersion("1.5.0", "1.6"));
        assertTrue(Version.isOlderMinorVersion("1.5.7", "1.6"));
        assertTrue(Version.isOlderMinorVersion("1.5.2", "1.6.1"));

        assertTrue(Version.isOlderMinorVersion("1.5", "1.6.0"));
        assertTrue(Version.isOlderMinorVersion("1.5.0", "1.6.0"));
        assertTrue(Version.isOlderMinorVersion("1.5.7", "1.6.0"));
        assertTrue(Version.isOlderMinorVersion("1.5.2", "1.6"));

        assertFalse(Version.isOlderMinorVersion("1.6.1", "1.6.0"));
        assertFalse(Version.isOlderMinorVersion("1.6", "1.6"));
        assertFalse(Version.isOlderMinorVersion("1.6.0", "1.6"));
        assertFalse(Version.isOlderMinorVersion("1.6.0", "1.6.0"));
        assertFalse(Version.isOlderMinorVersion("1.6.15", "1.6.10"));
        assertFalse(Version.isOlderMinorVersion("1.6.15", "1.6"));
        assertFalse(Version.isOlderMinorVersion("1.7.10", "1.6.15"));
        assertFalse(Version.isOlderMinorVersion("2.6.0", "1.7.15"));
    }

}