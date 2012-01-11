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
 * under the License. 
 */

package org.jumpmind.symmetric.util;

import java.util.Random;

import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.service.IParameterService;

/**
 * Use runtime configuration specific seeding to get a random number for use in
 * time slotting nodes to help stagger load.
 */
public class RandomTimeSlot {

    int maxValue = -1;

    Random random;

    public RandomTimeSlot() {
        random = new Random();
    }

    public RandomTimeSlot(IParameterService parameterService) {
        long seed = fromExternalId(parameterService.getExternalId());
        random = new Random(seed);
        if (maxValue < 0) {
            maxValue = parameterService.getInt(ParameterConstants.JOB_RANDOM_MAX_START_TIME_MS);
        }
    }

    public RandomTimeSlot(String externalId, int maxValue) {
        this.maxValue = maxValue;
        random = new Random(fromExternalId(externalId));
    }

    private long fromExternalId(String externalId) {
        if (externalId != null) {
            return Math.abs(externalId.hashCode());
        } else {
            return Integer.MAX_VALUE;
        }
    }

    public int getRandomValueSeededByExternalId() {
        int nextValue = random.nextInt(maxValue);
        return nextValue == 0 ? 1 : nextValue;
    }

    public void setMaxValue(int maxValue) {
        this.maxValue = maxValue;
    }
}