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

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Date;
import java.util.Properties;

import javax.mail.Message.RecipientType;
import javax.mail.MessagingException;
import javax.mail.NoSuchProviderException;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

import org.jumpmind.properties.TypedProperties;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.service.IMailService;
import org.jumpmind.symmetric.service.IParameterService;

public class MailService extends AbstractService implements IMailService {

    protected static final String JAVAMAIL_HOST_NAME = "mail.host";
    protected static final String JAVAMAIL_TRANSPORT = "mail.transport";
    protected static final String JAVAMAIL_PORT_NUMBER = "mail.smtp.port";
    protected static final String JAVAMAIL_PORT_NUMBER_SSL = "mail.smtps.port";
    protected static final String JAVAMAIL_FROM = "mail.from";
    protected static final String JAVAMAIL_USE_STARTTLS = "mail.smtp.starttls.enable";
    protected static final String JAVAMAIL_USE_AUTH = "mail.smtp.auth";
    protected static final String JAVAMAIL_TRUST_HOST = "mail.smtp.ssl.trust";
    protected static final String JAVAMAIL_TRUST_HOST_SSL = "mail.smtps.ssl.trust";

    public MailService(IParameterService parameterService, ISymmetricDialect symmetricDialect) {
        super(parameterService, symmetricDialect);
    }
    
    public String sendEmail(String subject, String text, String recipients) {
        return sendEmail(subject, text, recipients, getJavaMailProperties(),
                parameterService.getString(ParameterConstants.SMTP_TRANSPORT, "smtp"),
                parameterService.is(ParameterConstants.SMTP_USE_AUTH, false),
                parameterService.getString(ParameterConstants.SMTP_USER),
                parameterService.getString(ParameterConstants.SMTP_PASSWORD));
    }

    public String sendEmail(String subject, String text, String recipients, TypedProperties prop) {
        return sendEmail(subject, text, recipients, getJavaMailProperties(prop),
                prop.get(ParameterConstants.SMTP_TRANSPORT, "smtp"),
                prop.is(ParameterConstants.SMTP_USE_AUTH, false),
                prop.get(ParameterConstants.SMTP_USER), prop.get(ParameterConstants.SMTP_PASSWORD));        
    }

    protected String sendEmail(String subject, String text, String recipients, Properties prop, 
            String transportType, boolean useAuth, String user, String password) {
        Session session = Session.getInstance(prop);
        ByteArrayOutputStream ba = null;
        if (log.isDebugEnabled()) {
            session.setDebug(true);
            ba = new ByteArrayOutputStream();
            PrintStream ps = new PrintStream(ba);
            session.setDebugOut(ps);
        }
        Transport transport;
        try {
            transport = session.getTransport(transportType);
        } catch (NoSuchProviderException e) {
            log.error("Failure while obtaining transport", e);
            return getNestedErrorMessage(e);
        }

        try {
            if (useAuth) {
                transport.connect(user, password);
            } else {
                transport.connect();
            }
        } catch (MessagingException e) {
            log.error("Failure while connecting to transport", e);
            return getNestedErrorMessage(e);
        }

        try {
            MimeMessage message = new MimeMessage(session);
            message.setSentDate(new Date());
            message.setRecipients(RecipientType.TO, recipients);
            message.setSubject(subject);
            message.setText(text);
            message.setFrom(new InternetAddress(prop.getProperty(JAVAMAIL_FROM)));
            try {
                transport.sendMessage(message, message.getAllRecipients());
            } catch (MessagingException e) {
                log.error("Failure while sending notification", e);
                return getNestedErrorMessage(e);
            }
        } catch (MessagingException e) {
            log.error("Failure while preparing notification", e);
            return e.getMessage();
        } finally {
            try {
                transport.close();
            } catch (MessagingException e) {
            }
        }
        if (log.isDebugEnabled()) {
            log.debug(ba.toString());
        }
        return null;
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
            error = getNestedErrorMessage(e);
        } catch (MessagingException e) {
            error = getNestedErrorMessage(e);
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

    protected String getNestedErrorMessage(Exception e) {
        String error = e.getMessage();
        Throwable e2 = e.getCause();
        if (e2 != null) {
            error += "\n" + e2.getMessage();
            Throwable e3 = e2.getCause();    
            if (e3 != null) {
                error += "\n" + e3.getMessage();
            }
        }
        return error;
    }
    
    protected Properties getJavaMailProperties() {
        Properties prop = new Properties();
        prop.setProperty(JAVAMAIL_HOST_NAME, parameterService.getString(ParameterConstants.SMTP_HOST, "localhost"));
        prop.setProperty(JAVAMAIL_PORT_NUMBER, parameterService.getString(ParameterConstants.SMTP_PORT, "25"));
        prop.setProperty(JAVAMAIL_PORT_NUMBER_SSL, parameterService.getString(ParameterConstants.SMTP_PORT, "25"));
        prop.setProperty(JAVAMAIL_FROM, parameterService.getString(ParameterConstants.SMTP_FROM, "root@localhost"));
        prop.setProperty(JAVAMAIL_USE_STARTTLS, parameterService.getString(ParameterConstants.SMTP_USE_STARTTLS, "false"));
        prop.setProperty(JAVAMAIL_USE_AUTH, parameterService.getString(ParameterConstants.SMTP_USE_AUTH, "false"));
        prop.setProperty(JAVAMAIL_TRUST_HOST, parameterService.is(ParameterConstants.SMTP_ALLOW_UNTRUSTED_CERT, false) ? "*" : "");
        prop.setProperty(JAVAMAIL_TRUST_HOST_SSL, parameterService.is(ParameterConstants.SMTP_ALLOW_UNTRUSTED_CERT, false) ? "*" : "");
        return prop;
    }
    
    protected Properties getJavaMailProperties(TypedProperties typedProp) {
        Properties prop = new Properties();
        prop.setProperty(JAVAMAIL_HOST_NAME, typedProp.get(ParameterConstants.SMTP_HOST, "localhost"));
        prop.setProperty(JAVAMAIL_PORT_NUMBER, typedProp.get(ParameterConstants.SMTP_PORT, "25"));
        prop.setProperty(JAVAMAIL_PORT_NUMBER_SSL, typedProp.get(ParameterConstants.SMTP_PORT, "25"));
        prop.setProperty(JAVAMAIL_FROM, typedProp.get(ParameterConstants.SMTP_FROM, "root@localhost"));
        prop.setProperty(JAVAMAIL_USE_STARTTLS, String.valueOf(typedProp.is(ParameterConstants.SMTP_USE_STARTTLS, false)));
        prop.setProperty(JAVAMAIL_USE_AUTH, String.valueOf(typedProp.is(ParameterConstants.SMTP_USE_AUTH, false)));
        prop.setProperty(JAVAMAIL_TRUST_HOST, typedProp.is(ParameterConstants.SMTP_ALLOW_UNTRUSTED_CERT, false) ? "*" : "");
        prop.setProperty(JAVAMAIL_TRUST_HOST_SSL, typedProp.is(ParameterConstants.SMTP_ALLOW_UNTRUSTED_CERT, false) ? "*" : "");
        return prop;
    }

}
