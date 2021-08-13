/**
 * Licensed to JumpMind Inc under one or more contributor
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU General Public License, version 3.0 (GPLv3)
 * (the "License"); you may not use this file except in compliance
 * with the License.
 *
 * You should have received a copy of the GNU General Public License,
 * version 3.0 (GPLv3) along with this library; if not, see
 * <http://www.gnu.org/licenses/>.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.jumpmind.symmetric.monitor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jumpmind.db.model.Transaction;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.extension.IBuiltInExtensionPoint;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.model.Monitor;
import org.jumpmind.symmetric.model.MonitorEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

public class MonitorTypeBlock extends AbstractMonitorType implements IBuiltInExtensionPoint {
    protected final Logger log = LoggerFactory.getLogger(getClass());

    @Override
    public MonitorEvent check(Monitor monitor) {
        MonitorEvent event = new MonitorEvent();
        List<Transaction> transactions = null;
        IDatabasePlatform platform = engine.getDatabasePlatform();
        if (platform != null) {
            transactions = platform.getTransactions();
        }
        if (transactions != null && !transactions.isEmpty()) {
            Map<String, Transaction> transactionMap = new HashMap<String, Transaction>();
            for (Transaction transaction : transactions) {
                String id = transaction.getId();
                if (!transactionMap.containsKey(id) || transactionMap.get(transaction.getBlockingId()) == null) {
                    transactionMap.put(id, transaction);
                }
            }
            List<Transaction> filteredTransactions = new ArrayList<Transaction>();
            String dbUser = engine.getParameterService().getString(ParameterConstants.DB_USER);
            for (Transaction transaction : transactions) {
                filterTransactions(transaction, transactionMap, filteredTransactions, dbUser, false, false);
            }
            long secondsBlocked = 0;
            for (Transaction transaction : filteredTransactions) {
                if (transaction.getUsername().equals(dbUser) && transactionMap.get(transaction.getBlockingId()) != null) {
                    secondsBlocked = Math.max(secondsBlocked, transaction.getDuration() / 1000);
                }
            }
            event.setValue(secondsBlocked);
            if (secondsBlocked > 0) {
                event.setDetails(serializeDetails(filteredTransactions));
            }
        } else {
            event.setValue(0);
        }
        return event;
    }

    public static boolean filterTransactions(Transaction transaction, Map<String, Transaction> transactionMap,
            List<Transaction> filteredTransactions, String dbUser, boolean isBlockingUser, boolean isBlocking) {
        Transaction blockingTransaction = transactionMap.get(transaction.getBlockingId());
        if (!isBlocking && blockingTransaction == null) {
            return false;
        }
        if (filteredTransactions.contains(transaction)) {
            return true;
        }
        if (isBlockingUser || (dbUser != null && dbUser.equalsIgnoreCase(transaction.getUsername()))) {
            filteredTransactions.add(transaction);
            if (blockingTransaction != null) {
                filterTransactions(blockingTransaction, transactionMap, filteredTransactions, dbUser, true, true);
            }
            return true;
        }
        if (blockingTransaction != null
                && filterTransactions(blockingTransaction, transactionMap, filteredTransactions, dbUser, false, true)) {
            filteredTransactions.add(transaction);
            return true;
        }
        return false;
    }

    protected String serializeDetails(List<Transaction> transactions) {
        String result = null;
        try {
            result = new Gson().toJson(transactions);
        } catch (Exception e) {
            log.warn("Unable to convert list of transactions to JSON", e);
        }
        return result;
    }

    @Override
    public String getName() {
        return "block";
    }

    @Override
    public boolean requiresClusterLock() {
        return false;
    }
}
