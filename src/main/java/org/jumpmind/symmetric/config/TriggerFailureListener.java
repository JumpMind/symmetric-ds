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

import java.util.HashMap;
import java.util.Map;

import org.jumpmind.symmetric.model.Trigger;

/**
 * A listener that was built specifically to 'listen' for failures.
 */
public class TriggerFailureListener extends TriggerCreationAdapter {

    Map<Trigger, Exception> failures;

    public TriggerFailureListener() {
        this.failures = new HashMap<Trigger, Exception>();
    }

    @Override
    public void triggerFailed(Trigger trigger, Exception ex) {
        this.failures.put(trigger, ex);
    }

    public Map<Trigger, Exception> getFailures() {
        return failures;
    }

    public void setFailures(Map<Trigger, Exception> failures) {
        this.failures = failures;
    }
}
