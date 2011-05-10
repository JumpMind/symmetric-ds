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
package org.jumpmind.symmetric.util;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Throttle output stream to write at a specified rate. the rate will be an
 * average
 */
public class MeteredOutputStream extends FilterOutputStream {

    // max allowed, this will be an average over time. for small packages, it
    // will be consume the whole bandwidth
    private long maxBps;

    // total bytes send through
    private long bytesSent;

    // start time when stream started output
    private long startTime;

    // flag to determine whether stream started output
    private boolean started = false;

    // threshold before throttling in number of bytes
    private long threshold = DEFFAULT_THRESHOLD;

    // frequency to recalculation rate in number of bytes
    private long checkPoint = DEFAULT_CHECK_POINT;

    // default threshold before throttling in number of bytes
    private static final long DEFFAULT_THRESHOLD = 8192L;

    // default frequency to recalculation rate in number of bytes
    private static final long DEFAULT_CHECK_POINT = 1024L;

    /**
     * @param out
     *                stream written to
     * @param maxBps
     *                max number of bytes per second
     * @param threshold
     *                the number in bytes before the throttle output stream
     */
    public MeteredOutputStream(OutputStream out, long maxBps, long threshold) {
        super(out);
        this.maxBps = maxBps;
        bytesSent = 0;
        this.threshold = threshold <= DEFFAULT_THRESHOLD ? DEFFAULT_THRESHOLD : threshold;

        checkPoint = DEFAULT_CHECK_POINT;
    }

    /**
     * @param out
     *                out stream written to
     * @param maxBps
     *                max number of bytes per second
     * @param threshold
     *                the number in bytes before throttling output stream
     * @param checkPoint
     *                check the average rate when total byts%checkPoint == 0.
     *                <br>
     *                the throttled output stream will write checkPoing number
     *                of bytes at full speed, and then sleep for a certain to
     *                obtain an average close to maxBps. If set it to 1, it will
     *                check every bytes written and the rate will be the most
     *                accurate.
     */
    public MeteredOutputStream(OutputStream out, long maxBps, long threshold, long checkPoint) {
        super(out);
        this.maxBps = maxBps;
        bytesSent = 0;
        this.threshold = threshold <= DEFFAULT_THRESHOLD ? DEFFAULT_THRESHOLD : threshold;
        this.checkPoint = checkPoint;
    }

    /**
     * @param out
     * @param maxBps
     */
    public MeteredOutputStream(OutputStream out, long maxBps) {
        super(out);
        this.maxBps = maxBps;
        bytesSent = 0;
        this.threshold = DEFFAULT_THRESHOLD;
        checkPoint = DEFAULT_CHECK_POINT;
    }

    @Override
    public void write(int b) throws IOException {
        // check if stream start output, if not, set started to true and set the
        // start time
        bytesSent += 1;
        if (!started) {
            started = true;
            startTime = System.currentTimeMillis();
        }
        // only check when total bytes greater than limit and adjust at
        // checkPoint
        if (bytesSent >= threshold && (bytesSent % checkPoint == 0)) {
            long elapsed = System.currentTimeMillis() - startTime + 1;
            long currentBps = bytesSent * 1000L / elapsed;
            if (currentBps > maxBps) {

                long expected = bytesSent * 1000L / maxBps;
                try {
                    Thread.sleep(expected - elapsed);
                } catch (InterruptedException e) {
                }
            }
        }
        out.write(b);

    }
}