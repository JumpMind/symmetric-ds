package org.jumpmind.symmetric.service.jmx;

import javax.management.Notification;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.service.INotificationService;
import org.springframework.jmx.export.annotation.ManagedResource;
import org.springframework.jmx.export.notification.NotificationPublisher;
import org.springframework.jmx.export.notification.NotificationPublisherAware;

@ManagedResource(description = "Provide an implementation of SymmetricDS notifications by JMX")
public class NotificationService implements NotificationPublisherAware, INotificationService {

    static final Log logger = LogFactory.getLog(NotificationService.class);
    
    NotificationPublisher notificationPublisher;

    public void setNotificationPublisher(NotificationPublisher notificationPublisher) {
        this.notificationPublisher = notificationPublisher;
    }

    public void sendNotification(Notification event) {
        logger.warn("Sending a JMX notification with a message payload of: " + event.getMessage());
        this.notificationPublisher.sendNotification(event);
    }

}
