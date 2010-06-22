package org.jumpmind.symmetric.service.jmx;

import javax.management.Notification;

import org.jumpmind.symmetric.common.logging.ILog;
import org.jumpmind.symmetric.common.logging.LogFactory;
import org.jumpmind.symmetric.service.INotificationService;
import org.springframework.jmx.export.annotation.ManagedResource;
import org.springframework.jmx.export.notification.NotificationPublisher;
import org.springframework.jmx.export.notification.NotificationPublisherAware;

@ManagedResource(description = "Provide an implementation of SymmetricDS notifications by JMX")
public class NotificationService implements NotificationPublisherAware, INotificationService {

    static final ILog log = LogFactory.getLog(NotificationService.class);

    NotificationPublisher notificationPublisher;

    public void setNotificationPublisher(NotificationPublisher notificationPublisher) {
        this.notificationPublisher = notificationPublisher;
    }

    public void sendNotification(Notification event) {
        log.warn("JMXNotificationSending", event.getMessage());
        this.notificationPublisher.sendNotification(event);
    }

}
