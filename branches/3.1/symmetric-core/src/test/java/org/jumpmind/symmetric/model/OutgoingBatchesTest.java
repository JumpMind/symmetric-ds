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
 * under the License.  */
package org.jumpmind.symmetric.model;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.jumpmind.symmetric.model.OutgoingBatch.Status;
import org.jumpmind.symmetric.util.AppUtils;
import org.junit.Assert;
import org.junit.Test;

public class OutgoingBatchesTest {

    public static final String[] nodeIds = { "0001", "0002", "0003", "0004", "0005" };

    @Test
    public void testBasicFunctions() {
        OutgoingBatches batches = buildSampleBatches("testChannel", 5);
        Assert.assertNotNull(batches);
        Assert.assertEquals(25, batches.getBatches().size());
        Assert.assertEquals(0, batches.getActiveChannels().size());

        batches.addActiveChannel(new NodeChannel("testChannel1"));
        batches.addActiveChannel(new NodeChannel("testChannel2"));

        Assert.assertEquals(2, batches.getActiveChannels().size());
    }

    @Test
    public void testBasicGetters() {
        OutgoingBatches batches = buildSampleBatches("testChannel", 5);
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
        OutgoingBatches batches = buildSampleBatches("testChannel", 5);
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

        batches = buildSampleBatches("testChannel", 5);
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

    @Test
    public void testChannelSortingNoErrors() {
        List<NodeChannel> channels = new ArrayList<NodeChannel>();
        NodeChannel channelA = new NodeChannel("a");
        channelA.setProcessingOrder(1);

        NodeChannel channelB = new NodeChannel("b");
        channelB.setProcessingOrder(2);

        NodeChannel channelC = new NodeChannel("c");
        channelC.setProcessingOrder(3);

        channels.add(channelC);
        channels.add(channelB);
        channels.add(channelA);

        List<OutgoingBatch> batches = new ArrayList<OutgoingBatch>();
        OutgoingBatch batch1 = new OutgoingBatch("1", channelA.getChannelId(), Status.NE);
        batches.add(batch1);

        OutgoingBatch batch2 = new OutgoingBatch("1", channelB.getChannelId(), Status.NE);
        batches.add(batch2);

        OutgoingBatch batch3 = new OutgoingBatch("1", channelC.getChannelId(), Status.NE);
        batches.add(batch3);

        OutgoingBatches outgoingBatches = new OutgoingBatches(batches);

        outgoingBatches.sortChannels(channels);

        Assert.assertEquals(channelA, channels.get(0));
        Assert.assertEquals(channelB, channels.get(1));
        Assert.assertEquals(channelC, channels.get(2));
    }

    @Test
    public void testChannelSortingOneErrors() {
        List<NodeChannel> channels = new ArrayList<NodeChannel>();
        NodeChannel channelA = new NodeChannel("a");
        channelA.setProcessingOrder(1);

        NodeChannel channelB = new NodeChannel("b");
        channelB.setProcessingOrder(2);

        NodeChannel channelC = new NodeChannel("c");
        channelC.setProcessingOrder(3);

        channels.add(channelC);
        channels.add(channelB);
        channels.add(channelA);

        List<OutgoingBatch> batches = new ArrayList<OutgoingBatch>();
        OutgoingBatch batch1 = new OutgoingBatch("1", channelA.getChannelId(), Status.NE);
        batch1.setStatus(OutgoingBatch.Status.ER);
        batch1.setErrorFlag(true);
        batches.add(batch1);

        OutgoingBatch batch2 = new OutgoingBatch("1", channelB.getChannelId(), Status.NE);
        batches.add(batch2);

        OutgoingBatch batch3 = new OutgoingBatch("1", channelC.getChannelId(), Status.NE);
        batches.add(batch3);

        OutgoingBatches outgoingBatches = new OutgoingBatches(batches);

        outgoingBatches.sortChannels(channels);

        Assert.assertEquals(channelB, channels.get(0));
        Assert.assertEquals(channelC, channels.get(1));
        Assert.assertEquals(channelA, channels.get(2));
    }

    @Test
    public void testChannelSortingTwoErrors() {
        List<NodeChannel> channels = new ArrayList<NodeChannel>();
        NodeChannel channelA = new NodeChannel("a");
        channelA.setProcessingOrder(1);

        NodeChannel channelB = new NodeChannel("b");
        channelB.setProcessingOrder(2);

        NodeChannel channelC = new NodeChannel("c");
        channelC.setProcessingOrder(3);

        channels.add(channelC);
        channels.add(channelB);
        channels.add(channelA);

        List<OutgoingBatch> batches = new ArrayList<OutgoingBatch>();
        OutgoingBatch batch1 = new OutgoingBatch("1", channelA.getChannelId(), Status.NE);
        batch1.setStatus(OutgoingBatch.Status.ER);
        batch1.setErrorFlag(true);
        batch1.setLastUpdatedTime(new Date());
        batches.add(batch1);

        AppUtils.sleep(50);

        OutgoingBatch batch2 = new OutgoingBatch("1", channelB.getChannelId(), Status.NE);
        batch2.setStatus(OutgoingBatch.Status.ER);
        batch2.setErrorFlag(true);
        batch2.setLastUpdatedTime(new Date());
        batches.add(batch2);

        OutgoingBatch batch3 = new OutgoingBatch("1", channelC.getChannelId(), Status.NE);
        batches.add(batch3);

        OutgoingBatches outgoingBatches = new OutgoingBatches(batches);

        outgoingBatches.sortChannels(channels);

        Assert.assertEquals(channelC, channels.get(0));
        Assert.assertEquals(channelA, channels.get(1));
        Assert.assertEquals(channelB, channels.get(2));

        AppUtils.sleep(50);

        batch1.setLastUpdatedTime(new Date());

        outgoingBatches.sortChannels(channels);

        Assert.assertEquals(channelC, channels.get(0));
        Assert.assertEquals(channelB, channels.get(1));
        Assert.assertEquals(channelA, channels.get(2));
    }

    protected OutgoingBatches buildSampleBatches(String channelId, int batchCount) {
        OutgoingBatches outgoingBatches = new OutgoingBatches();
        List<OutgoingBatch> batches = new ArrayList<OutgoingBatch>();

        int batchId = 1;

        for (int i = 0; i < 5; i++) {
            for (int j = 0; j < 5; j++) {
                OutgoingBatch b = new OutgoingBatch(nodeIds[i], "testChannel" + j, Status.NE);
                b.setBatchId(batchId++);
                batches.add(b);
            }
        }
        outgoingBatches.setBatches(batches);
        return outgoingBatches;
    }
}