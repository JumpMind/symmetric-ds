package org.jumpmind.symmetric.model;

import java.sql.Time;
import java.util.Calendar;

import org.junit.Assert;
import org.junit.Test;

public class NodeGroupChannelWindowTest {

    @Test
    public void testInWindowEnabled() {
        NodeGroupChannelWindow window = new NodeGroupChannelWindow();
        window.setEnabled(true);
        window.setStartTime(Time.valueOf("13:00:00"));
        window.setEndTime(Time.valueOf("14:00:00"));
        Calendar cal = Calendar.getInstance();
        cal.set(Calendar.HOUR_OF_DAY, 13);
        cal.set(Calendar.MINUTE, 5);
        Assert.assertTrue(window.inTimeWindow(cal.getTime()));
        cal.add(Calendar.DATE, 1);
        Assert.assertTrue(window.inTimeWindow(cal.getTime()));
        cal.add(Calendar.DATE, -100);
        Assert.assertTrue(window.inTimeWindow(cal.getTime()));
    }
    
    @Test
    public void testInWindowEnabledCrossDayBoundary() {
        NodeGroupChannelWindow window = new NodeGroupChannelWindow();
        window.setEnabled(true);
        window.setStartTime(Time.valueOf("21:00:00"));
        window.setEndTime(Time.valueOf("03:00:00"));
        Calendar cal = Calendar.getInstance();
        cal.set(Calendar.HOUR_OF_DAY, 13);
        cal.set(Calendar.MINUTE, 5);
        Assert.assertFalse(window.inTimeWindow(cal.getTime()));
        cal.set(Calendar.HOUR_OF_DAY, 22);
        Assert.assertTrue(window.inTimeWindow(cal.getTime()));
        cal.set(Calendar.HOUR_OF_DAY, 2);
        Assert.assertTrue(window.inTimeWindow(cal.getTime()));        
    }    
    
    @Test
    public void testInWindowDisabled() {
        NodeGroupChannelWindow window = new NodeGroupChannelWindow();
        window.setEnabled(false);
        window.setStartTime(Time.valueOf("13:00:00"));
        window.setEndTime(Time.valueOf("14:00:00"));
        Calendar cal = Calendar.getInstance();
        cal.set(Calendar.HOUR_OF_DAY, 13);
        cal.set(Calendar.MINUTE, 5);
        Assert.assertTrue(window.inTimeWindow(cal.getTime()));
        cal.add(Calendar.DATE, 1);
        Assert.assertTrue(window.inTimeWindow(cal.getTime()));
        cal.add(Calendar.DATE, -100);
        Assert.assertTrue(window.inTimeWindow(cal.getTime()));
    }
    
    @Test
    public void testOutOfWindowDisabled() {
        NodeGroupChannelWindow window = new NodeGroupChannelWindow();
        window.setEnabled(false);
        window.setStartTime(Time.valueOf("13:00:00"));
        window.setEndTime(Time.valueOf("14:00:00"));
        Calendar cal = Calendar.getInstance();
        cal.set(Calendar.HOUR_OF_DAY, 17);
        cal.set(Calendar.MINUTE, 5);
        Assert.assertTrue(window.inTimeWindow(cal.getTime()));
        cal.add(Calendar.DATE, 1);
        Assert.assertTrue(window.inTimeWindow(cal.getTime()));
        cal.add(Calendar.DATE, -100);
        Assert.assertTrue(window.inTimeWindow(cal.getTime()));
    }
    
    @Test
    public void testOutOfWindowEnabled() {
        NodeGroupChannelWindow window = new NodeGroupChannelWindow();
        window.setEnabled(true);
        window.setStartTime(Time.valueOf("09:00:00"));
        window.setEndTime(Time.valueOf("09:30:00"));
        Calendar cal = Calendar.getInstance();
        cal.set(Calendar.HOUR_OF_DAY, 8);
        cal.set(Calendar.MINUTE, 59);
        cal.set(Calendar.SECOND, 30);
        Assert.assertFalse(window.inTimeWindow(cal.getTime()));
        cal.add(Calendar.DATE, 423);
        Assert.assertFalse(window.inTimeWindow(cal.getTime()));
        cal.add(Calendar.DATE, -1);
        Assert.assertFalse(window.inTimeWindow(cal.getTime()));
    }
    
    
    
}