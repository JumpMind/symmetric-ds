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
package org.jumpmind.symmetric.db;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.service.IParameterService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class SoftwareUpgradeListenerTest {
    private static final String CURRENT_SOFTWARE_VERSION = "3.16.14.1";
    private IParameterService parameterService;
    private SoftwareUpgradeListener listener;

    @BeforeEach
    public void setUp() {
        parameterService = mock(IParameterService.class);
        ISymmetricEngine engine = mock(ISymmetricEngine.class);
        when(engine.getParameterService()).thenReturn(parameterService);
        listener = new SoftwareUpgradeListener() {
            @Override
            protected void upgradeModules() {
            }
        };
        listener.setSymmetricEngine(engine);
    }

    @Test
    public void testStrandedDataRecaptureDisabledUpgradingFromOlderMinorVersion() {
        upgradeFrom("3.15.6");
        verifyStrandedDataRecaptureDisabled();
    }

    @Test
    public void testStrandedDataRecaptureDisabledUpgradingFromVersionExposedToStaleGaps() {
        upgradeFrom("3.16.8");
        verifyStrandedDataRecaptureDisabled();
    }

    @Test
    public void testStrandedDataRecaptureLeftAloneUpgradingFromBaseVersion() {
        upgradeFrom("3.16.14");
        verifyStrandedDataRecaptureUnchanged();
    }

    @Test
    public void testStrandedDataRecaptureLeftAloneUpgradingFromNewerVersion() {
        upgradeFrom("3.16.18");
        verifyStrandedDataRecaptureUnchanged();
    }

    private void upgradeFrom(String databaseVersion) {
        listener.upgrade(databaseVersion, CURRENT_SOFTWARE_VERSION);
    }

    private void verifyStrandedDataRecaptureDisabled() {
        verify(parameterService).saveParameter(ParameterConstants.PURGE_STRANDED_DATA_RECAPTURE_ENABLED, false, "upgrade");
    }

    private void verifyStrandedDataRecaptureUnchanged() {
        verify(parameterService, never()).saveParameter(ParameterConstants.PURGE_STRANDED_DATA_RECAPTURE_ENABLED, false, "upgrade");
    }
}
