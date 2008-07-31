package org.jumpmind.symmetric.service;

import javax.management.Notification;

public interface INotificationService {

    public void sendNotification(Notification event);

}
