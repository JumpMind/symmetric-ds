package org.jumpmind.util;

import java.util.Random;

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

}