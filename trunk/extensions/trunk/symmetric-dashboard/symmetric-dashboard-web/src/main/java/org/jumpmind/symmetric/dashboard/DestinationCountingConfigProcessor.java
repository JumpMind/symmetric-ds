package org.jumpmind.symmetric.dashboard;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.flex.config.MessageBrokerConfigProcessor;

import flex.messaging.MessageBroker;
import flex.messaging.services.RemotingService;

public class DestinationCountingConfigProcessor implements
		MessageBrokerConfigProcessor {

	private static final Log logger = LogFactory.getLog(DestinationCountingConfigProcessor.class);
	
	public MessageBroker processAfterStartup(MessageBroker broker) {
		RemotingService remotingService = 
			(RemotingService) broker.getServiceByType(RemotingService.class.getName());
		if (remotingService.isStarted()) {
			logger.info("The Remoting Service has been started with "
					+remotingService.getDestinations().size()+" destinations.");
		}
		return broker;
	}

	public MessageBroker processBeforeStartup(MessageBroker broker) {
		return broker;
	}
}