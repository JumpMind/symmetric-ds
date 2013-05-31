package org.jumpmind.symmetric.transport.http;

import java.net.URI;
import java.util.Map;

import junit.framework.Assert;

import org.jumpmind.symmetric.service.IBandwidthService;
import org.jumpmind.symmetric.service.impl.MockNodeService;
import org.junit.Test;

public class HttpBandwidthBalancerTest {

    @Test
    public void testUriParsing() throws Exception {
        URI uri = new URI(
                "ext://plugin/?1=http://rgn.com/sync&2=http://rgn2.com/sync&sampleBytes=1000&sampleTTL=200&initialLoadOnly=true");
        HttpBandwidthUrlSelector ext = getMockBandwidthBalancer(true);
        Map<String, String> params = ext.getParameters(uri);
        Assert.assertEquals("http://rgn.com/sync", params.get("1"));
        Assert.assertEquals("http://rgn2.com/sync", params.get("2"));
        Assert.assertEquals("1000", params.get("sampleBytes"));
        Assert.assertEquals("200", params.get("sampleTTL"));
        Assert.assertEquals("true", params.get("initialLoadOnly"));
    }

    @Test
    public void testResolveUrl() throws Exception {
        HttpBandwidthUrlSelector ext = getMockBandwidthBalancer(false);
        URI uri = new URI("ext://balancer?10=100&100=50&"
                + HttpBandwidthUrlSelector.PARAM_PRELOAD_ONLY + "=true");
        Assert.assertEquals("50", ext.resolveUrl(uri));
        ext = getMockBandwidthBalancer(true);
        Assert.assertEquals("100", ext.resolveUrl(uri));
    }

    @Test
    public void testSampleTTL() throws Exception {
        HttpBandwidthUrlSelector ext = getMockBandwidthBalancer(true);
        URI uri = new URI("ext://balancer?1=100&2=1&" + HttpBandwidthUrlSelector.PARAM_SAMPLE_TTL
                + "=1000");
        Assert.assertEquals("1", ext.resolveUrl(uri));
        long ts = ext.lastSampleTs;
        Assert.assertEquals("1", ext.resolveUrl(uri));
        Assert.assertEquals(ts, ext.lastSampleTs);
        Thread.sleep(1000);
        Assert.assertEquals("1", ext.resolveUrl(uri));
        Assert.assertNotSame(ts, ext.lastSampleTs);
    }

    protected HttpBandwidthUrlSelector getMockBandwidthBalancer(final boolean dataLoadCompleted) {
        HttpBandwidthUrlSelector ext = new HttpBandwidthUrlSelector(
                new MockNodeService() {
                    @Override
                    public boolean isDataLoadCompleted() {
                        return dataLoadCompleted;
                    }
                }, new IBandwidthService() {
                    public double getDownloadKbpsFor(String url, long sampleSize,
                            long maxTestDuration) {
                        return sampleSize / Double.parseDouble(url);
                    }
                });
        return ext;
    }

}