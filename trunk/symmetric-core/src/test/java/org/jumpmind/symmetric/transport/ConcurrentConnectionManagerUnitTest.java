package org.jumpmind.symmetric.transport;

import java.util.HashMap;
import java.util.Map;

import junit.framework.Assert;

import org.jumpmind.symmetric.statistic.MockStatisticManager;
import org.jumpmind.symmetric.transport.ConcurrentConnectionManager.Reservation;
import org.jumpmind.symmetric.transport.IConcurrentConnectionManager.ReservationType;
import org.junit.Test;

public class ConcurrentConnectionManagerUnitTest {

    @Test
    public void testRemoveTimedOutReservations() {
        ConcurrentConnectionManager mgr = new ConcurrentConnectionManager(null, new MockStatisticManager());
        Map<String, Reservation> reservations = new HashMap<String, Reservation>();

        String nodeId = "1";
        Reservation current = new ConcurrentConnectionManager.Reservation(nodeId, System.currentTimeMillis()+10000, ReservationType.HARD);        
        reservations.put(nodeId, current);
        
        nodeId = "2";
        current = new ConcurrentConnectionManager.Reservation(nodeId,  System.currentTimeMillis()+10000, ReservationType.HARD);
        reservations.put(nodeId, current);

        Assert.assertEquals(2, reservations.size());
        mgr.removeTimedOutReservations(reservations);
        Assert.assertEquals(2, reservations.size());
        current.timeToLiveInMs = System.currentTimeMillis()-10000;
        mgr.removeTimedOutReservations(reservations);
        Assert.assertEquals(1, reservations.size());
    }
}