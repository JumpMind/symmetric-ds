package org.jumpmind.symmetric.transport.mock;

import org.jumpmind.symmetric.model.BatchInfo;
import org.jumpmind.symmetric.transport.IAcknowledgeEventListener;

import com.sun.org.apache.bcel.internal.generic.StoreInstruction;

public class MockAcknowledgeEventListener implements IAcknowledgeEventListener {

	public boolean isAutoRegister() {
		// TODO Auto-generated method stub
		return true;
	}

	public void onAcknowledgeEvent(BatchInfo batchInfo) {
		
	}
}
