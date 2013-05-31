package org.jumpmind.symmetric.model;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.lang.StringUtils;

/**
 * 
 */
public class ChannelMap {

    public static final String CHANNELS_SUSPEND = "Suspended-Channels";

    public static final String CHANNELS_IGNORE = "Ignored-Channels";

    private Map<String, Set<String>> map;

    public ChannelMap() {
        map = new HashMap<String, Set<String>>();

        Set<String> suspendChannels = new TreeSet<String>();
        map.put(CHANNELS_SUSPEND, suspendChannels);

        Set<String> ignoreChannels = new TreeSet<String>();
        map.put(CHANNELS_IGNORE, ignoreChannels);
    }

    public void addSuspendChannels(Collection<String> suspends) {
        if (suspends != null) {
            map.get(CHANNELS_SUSPEND).addAll(suspends);
        }
    }

    public void addIgnoreChannels(Collection<String> ignores) {
        if (ignores != null) {
            map.get(CHANNELS_IGNORE).addAll(ignores);
        }
    }

    public void addSuspendChannels(String suspends) {
        if (suspends != null) {
            map.get(CHANNELS_SUSPEND).addAll(Arrays.asList(suspends.split(",")));
        }
    }

    public void addIgnoreChannels(String ignores) {
        if (ignores != null) {
            map.get(CHANNELS_IGNORE).addAll(Arrays.asList(ignores.split(",")));
        }
    }

    public String getSuspendChannelsAsString() {
        return StringUtils.join(map.get(CHANNELS_SUSPEND), ',');
    }

    public String getIgnoreChannelsAsString() {
        return StringUtils.join(map.get(CHANNELS_IGNORE), ',');
    }

    public Set<String> getSuspendChannels() {
        return map.get(CHANNELS_SUSPEND);
    }

    public Set<String> getIgnoreChannels() {
        return map.get(CHANNELS_IGNORE);
    }
}