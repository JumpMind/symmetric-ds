package org.jumpmind.symmetric.android.example;

import junit.framework.Assert;

import org.jumpmind.symmetric.android.example.TestActivity;

import android.test.ActivityInstrumentationTestCase2;

public class SymmetricClientTest extends ActivityInstrumentationTestCase2<TestActivity> {

    public SymmetricClientTest() {
        super(TestActivity.class);
    }
    
    public void testInitialize() {
        TestActivity activity = getActivity();
        Assert.assertNotNull(activity.symmetricClient);
        activity.symmetricClient.initialize();
    }
}
