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
package org.jumpmind.symmetric.statistic;

public enum StatisticName {

    INCOMING_TRANSPORT_ERROR_COUNT, INCOMING_TRANSPORT_CONNECT_ERROR_COUNT, INCOMING_TRANSPORT_REJECTED_COUNT, INCOMING_DATABASE_ERROR_COUNT, INCOMING_OTHER_ERROR_COUNT, INCOMING_MS_PER_ROW, INCOMING_BATCH_COUNT, INCOMING_SKIP_BATCH_COUNT,

    OUTGOING_MS_PER_EVENT_BATCHED, OUTGOING_EVENTS_PER_BATCH,

    NODE_CONCURRENCY_TOO_BUSY_COUNT, NODE_CONCURRENCY_CONNECTION_RESERVED, NODE_CONCURRENCY_RESERVATION_REQUESTED, NODE_CONCURRENCY_RESERVATION_TIMEOUT_COUNT,

    OUTGOING_NETWORK_ERRORS, OUTGOING_DATABASE_ERRORS, OUTGOING_ROWS_PER_MS, OUTGOING_BATCH_COUNT, OUTGOING_ROW_COUNT
}
