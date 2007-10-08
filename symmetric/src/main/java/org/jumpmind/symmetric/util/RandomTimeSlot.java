/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Chris Henson <chenson42@users.sourceforge.net>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see
 * <http://www.gnu.org/licenses/>.
 */

package org.jumpmind.symmetric.util;

import java.util.Random;

import org.jumpmind.symmetric.config.IRuntimeConfig;

/**
 * Use runtime configuration specific seeding to get a random number for use in time 
 * slotting clients to help stagger load.
 */
public class RandomTimeSlot {

    IRuntimeConfig runtimeConfiguration;

    int maxValue = 1000;

    Random random;

    public RandomTimeSlot() {
        random = new Random();
    }

    public RandomTimeSlot(IRuntimeConfig config, int maxValue) {
        this.runtimeConfiguration = config;
        this.maxValue = maxValue;
        random = new Random(runtimeConfiguration.getExternalId().hashCode());
    }

    public void setMaxValue(int maxValue) {
        this.maxValue = maxValue;
    }

    public void setRuntimeConfiguration(IRuntimeConfig runtimeConfiguration) {
        this.runtimeConfiguration = runtimeConfiguration;
        random = new Random(runtimeConfiguration.getExternalId().hashCode());
    }

    public int getRandomValueSeededByDomainId() {
        return random.nextInt(maxValue);
    }
}
