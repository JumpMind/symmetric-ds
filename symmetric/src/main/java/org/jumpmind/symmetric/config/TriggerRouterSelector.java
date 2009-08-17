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
package org.jumpmind.symmetric.config;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.jumpmind.symmetric.model.TriggerRouter;

/**
 * Utility class to pair down a list of triggers.
 */
public class TriggerRouterSelector {

    private String channelId;
    private String targetNodeGroupId;
    private Collection<TriggerRouter> triggers;

    public TriggerRouterSelector(Collection<TriggerRouter> triggers, String channelId, String targetNodeGroupId) {
        this.triggers = triggers;
        this.channelId = channelId;
        this.targetNodeGroupId = targetNodeGroupId;
    }

    public List<TriggerRouter> select() {
        List<TriggerRouter> filtered = new ArrayList<TriggerRouter>();
        for (TriggerRouter trigger : triggers) {
            if (trigger.getTrigger().getChannelId().equals(channelId)
                    && (targetNodeGroupId == null || trigger.getRouter().getTargetGroupId().equals(targetNodeGroupId))) {
                filtered.add(trigger);
            }
        }
        return filtered;
    }
}
