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

package org.jumpmind.symmetric.transport;

/**
 * Copyright (C) 2005 Oliver Hitz <oliver@net-track.ch>
 *
 * $Id$
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston,
 * MA  02111-1307, USA.
 *
 * 
 */

/**
 * A very simple bandwidth meter. It calculates the mean of different
 * consecutive bandwidth measurements.
 */
public class BandwidthTestResults {
    long start;
    long total = 0;
    long duration = 0;

    /**
     * Starts the bandwidth measurement.
     */
    public void start() {
        start = System.currentTimeMillis();
    }

    /**
     * Called when a bunch of data has been transmitted.
     * 
     * @param n
     *            Number of bytes that have been transmitted.
     */
    public void transmitted(int n) {
        total += n;
    }

    /**
     * Ends the bandwidth measurement.
     */
    public void stop() {
        duration += System.currentTimeMillis() - start;
    }

    /**
     * Returns the bandwidth used in kbits/s beetween start() and stop().
     * 
     * @return Bandwidth in Kbps.
     */
    public double getKbps() {
        if (total == 0 || duration == 0) {
            return 0;
        } else {
            return (8.0d * 1000.0d * total / 1024.0d) / duration;
        }
    }

    /**
     * Returns the number of milliseconds elapsed between start() and stop().
     * 
     * @return Number of milliseconds elapsed between start() and stop().
     */
    public long getElapsed() {
        return duration;
    }
    
}