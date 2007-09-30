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
