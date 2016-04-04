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
package org.jumpmind.symmetric.service.impl;

import java.util.Date;
import java.util.Properties;

import javax.mail.Message.RecipientType;
import javax.mail.MessagingException;
import javax.mail.NoSuchProviderException;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.MimeMessage;

import org.jumpmind.properties.TypedProperties;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.service.IMailService;
import org.jumpmind.symmetric.service.IParameterService;

public class MailService extends AbstractService implements IMailService {

    protected static final String JAVAMAIL_HOST_NAME = "mail.host";
    protected static final String JAVAMAIL_TRANSPORT = "mail.transport";
    protected static final String JAVAMAIL_PORT_NUMBER = "mail.port";
    protected static final String JAVAMAIL_FROM = "mail.from";
    protected static final String JAVAMAIL_USERNAME = "mail.user";
    protected static final String JAVAMAIL_PASSWORD = "mail.password";
    protected static final String JAVAMAIL_USE_STARTTLS = "mail.smtp.starttls.enable";
    protected static final String JAVAMAIL_USE_AUTH = "mail.smtp.auth";

    public MailService(IParameterService parameterService, ISymmetricDialect symmetricDialect) {
        super(parameterService, symmetricDialect);
    }

    public void sendEmail(String subject, String text, String recipients) {
        Session session = Session.getInstance(getJavaMailProperties());
        Transport transport;
        try {
            transport = session.getTransport(parameterService.getString(ParameterConstants.SMTP_TRANSPORT, "smtp"));
        } catch (NoSuchProviderException e) {
            log.error("Failure while obtaining transport", e);
            return;
        }

        try {
            if (parameterService.is(ParameterConstants.SMTP_USE_AUTH, false)) {
                transport.connect(parameterService.getString(ParameterConstants.SMTP_USER),
                        parameterService.getString(ParameterConstants.SMTP_PASSWORD));
            } else {
                transport.connect();
            }
        } catch (MessagingException e) {
            log.error("Failure while connecting to transport", e);
            return;
        }

        try {
            MimeMessage message = new MimeMessage(session);
            message.setSentDate(new Date());
            message.setRecipients(RecipientType.BCC, recipients);
            message.setSubject(subject);
            message.setText(text);
            try {
                transport.sendMessage(message, message.getAllRecipients());
            } catch (MessagingException e) {
                log.error("Failure while sending notification", e);
            }
        } catch (MessagingException e) {
            log.error("Failure while preparing notification", e);
        } finally {
            try {
                transport.close();
            } catch (MessagingException e) {
            }
        }
    }

    public String testTransport(TypedProperties prop) {
        String error = null;
        Transport transport = null;
        try {
            Session session = Session.getInstance(getJavaMailProperties(prop));
            transport = session.getTransport(prop.get(ParameterConstants.SMTP_TRANSPORT, "smtp"));
            if (prop.is(ParameterConstants.SMTP_USE_AUTH, false)) {
                transport.connect(prop.get(ParameterConstants.SMTP_USER),
                        prop.get(ParameterConstants.SMTP_PASSWORD));
            } else {
                transport.connect();
            }
        } catch (NoSuchProviderException e) {
            error = e.getMessage();
        } catch (MessagingException e) {
            error = e.getMessage();
        } finally {
            try {
                if (transport != null) {
                    transport.close();
                }
            } catch (MessagingException e) {
            }
        }
        return error;
    }

    protected Properties getJavaMailProperties() {
        Properties prop = new Properties();
        prop.setProperty(JAVAMAIL_HOST_NAME, parameterService.getString(ParameterConstants.SMTP_HOST, "localhost"));
        prop.setProperty(JAVAMAIL_PORT_NUMBER, parameterService.getString(ParameterConstants.SMTP_PORT, "25"));
        prop.setProperty(JAVAMAIL_FROM, parameterService.getString(ParameterConstants.SMTP_FROM, "root@localhost"));
        prop.setProperty(JAVAMAIL_USE_STARTTLS, parameterService.getString(ParameterConstants.SMTP_USE_STARTTLS, "false"));
        prop.setProperty(JAVAMAIL_USE_AUTH, parameterService.getString(ParameterConstants.SMTP_USE_AUTH, "false"));
        return prop;
    }
    
    protected Properties getJavaMailProperties(TypedProperties typedProp) {
        Properties prop = new Properties();
        prop.setProperty(JAVAMAIL_HOST_NAME, typedProp.get(ParameterConstants.SMTP_HOST, "localhost"));
        prop.setProperty(JAVAMAIL_PORT_NUMBER, typedProp.get(ParameterConstants.SMTP_PORT, "25"));
        prop.setProperty(JAVAMAIL_FROM, typedProp.get(ParameterConstants.SMTP_FROM, "root@localhost"));
        prop.setProperty(JAVAMAIL_USE_STARTTLS, String.valueOf(typedProp.is(ParameterConstants.SMTP_USE_STARTTLS, false)));
        prop.setProperty(JAVAMAIL_USE_AUTH, String.valueOf(typedProp.is(ParameterConstants.SMTP_USE_AUTH, false)));
        return prop;
    }

}
