package org.jumpmind.symmetric.model;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

public class OutgoingBatchesTest {

    public static final String[] nodeIds = { "0001", "0002", "0003", "0004", "0005" };

    @Test
    public void testBasicFunctions() {
        OutgoingBatches batches = buildSampleBatches();
        Assert.assertNotNull(batches);
        Assert.assertEquals(25, batches.getBatches().size());
        Assert.assertEquals(0, batches.getActiveChannels().size());

        batches.addActiveChannel(new NodeChannel("testChannel1"));
        batches.addActiveChannel(new NodeChannel("testChannel2"));

        Assert.assertEquals(2, batches.getActiveChannels().size());
    }

    @Test
    public void testBasicGetters() {
        OutgoingBatches batches = buildSampleBatches();
        Assert.assertNotNull(batches);
        Assert.assertEquals(25, batches.getBatches().size());
        Assert.assertEquals(0, batches.getActiveChannels().size());

        List<OutgoingBatch> batchList = batches.getBatchesForChannel("testChannel2");
        Assert.assertEquals(5, batchList.size());
        int i = 3;
        for (OutgoingBatch b : batchList) {
            Assert.assertEquals(b.getChannelId(), "testChannel2");
            Assert.assertEquals(i, b.getBatchId());
            i += 5;
        }

        batchList = batches.getBatchesForChannel(new Channel("testChannel1", 1));
        Assert.assertEquals(5, batchList.size());
        i = 2;
        for (OutgoingBatch b : batchList) {
            Assert.assertEquals(b.getChannelId(), "testChannel1");
            Assert.assertEquals(i, b.getBatchId());
            i += 5;
        }
        Set<String> channels = new HashSet<String>();

        channels.add("testChannel2");
        channels.add("testChannel3");
        channels.add("testChannel4");

        batchList = batches.getBatchesForChannels(channels);
        Assert.assertEquals(15, batchList.size());

    }

    @Test
    public void testWindowGetter() {

        // TODO
    }

    @Test
    public void testFilters() {
        OutgoingBatches batches = buildSampleBatches();
        Assert.assertNotNull(batches);
        Assert.assertEquals(25, batches.getBatches().size());
        Assert.assertEquals(0, batches.getActiveChannels().size());

        batches.filterBatchesForChannel("testChannel3");
        Assert.assertEquals(20, batches.getBatches().size());

        for (OutgoingBatch b : batches.getBatches()) {
            Assert.assertFalse(b.getChannelId().equals("testChannel3"));
        }

        batches.filterBatchesForChannel(new Channel("testChannel4", 1));
        Assert.assertEquals(15, batches.getBatches().size());

        for (OutgoingBatch b : batches.getBatches()) {
            Assert.assertFalse(b.getChannelId().equals("testChannel3"));
            Assert.assertFalse(b.getChannelId().equals("testChannel4"));
        }
        Set<String> channels = new HashSet<String>();

        channels.add("testChannel2");
        channels.add("testChannel5");
        batches.filterBatchesForChannels(channels);

        Assert.assertEquals(10, batches.getBatches().size());

        for (OutgoingBatch b : batches.getBatches()) {
            Assert.assertTrue(b.getChannelId().equals("testChannel1") || b.getChannelId().equals("testChannel0"));
        }

        batches = buildSampleBatches();
        batches.addActiveChannel(new NodeChannel("testChannel2"));
        batches.addActiveChannel(new NodeChannel("testChannel3"));
        batches.addActiveChannel(new NodeChannel("testChannel4"));

        batches.filterBatchesForInactiveChannels();
        Assert.assertEquals(15, batches.getBatches().size());
        for (OutgoingBatch b : batches.getBatches()) {
            Assert.assertTrue(b.getChannelId().equals("testChannel2") || b.getChannelId().equals("testChannel3")
                    || b.getChannelId().equals("testChannel4"));
        }
    }

    public OutgoingBatches buildSampleBatches() {
        OutgoingBatches outgoingBatches = new OutgoingBatches();
        List<OutgoingBatch> batches = new ArrayList<OutgoingBatch>();

        int batchId = 1;

        for (int i = 0; i < 5; i++) {
            for (int j = 0; j < 5; j++) {
                OutgoingBatch b = new OutgoingBatch(nodeIds[i], "testChannel" + j);
                b.setBatchId(batchId++);
                batches.add(b);
            }
        }
        outgoingBatches.setBatches(batches);
        return outgoingBatches;
    }
}
