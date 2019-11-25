package com.confluent.connect;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Importance;
import java.util.Map;
import java.util.Properties;

public class EmailSinkConnectorConfig extends AbstractConfig {
    public static final String USER_NAME = "username";
    private static final String USER_NAME_DOC = "SMTP username.";
    public static final String PASSWORD = "password";
    private static final String PASSWORD_DOC = "SMTP password.";
    public static final String SMTP_HOST = "mail.smtp.host";
    private static final String SMTP_HOST_DOC = "SMTP host.";
    public static final String SMTP_PORT = "mail.smtp.port";
    private static final String SMTP_PORT_DOC = "SMTP port.";
    public static final String SMTP_AUTH = "mail.smtp.auth";
    private static final String SMTP_AUTH_DOC = "SMTP Auth required?";
    public static final String TLS_ENABLED = "mail.smtp.starttls.enable";
    private static final String TLS_ENABLED_DOC = "TLS enabled?";
    public static final String SMTP_FROM = "mail.smtp.from";
    private static final String SMTP_FROM_DOC = "From Email Address.";
    public static final String SMTP_TO = "mail.smtp.to";
    private static final String SMTP_TO_DOC = "Coma Seperated Recepient Email Addresses.";
    public static final String SMTP_SUBJECT = "mail.smtp.subject";
    private static final String SMTP_SUBJECT_DOC = "Email Subject.";
    public static final String SMTP_BODY_TEMPLATE = "mail.smtp.body.template";
    private static final String SMTP_BODY_TEMPLATE_DOC = "Email Body Template.";

    public EmailSinkConnectorConfig(ConfigDef config, Map<String, String> parsedConfig) {
        super(config, parsedConfig);
    }

    public EmailSinkConnectorConfig(Map<String, String> parsedConfig) {
        this(conf(), parsedConfig);
    }

    public static ConfigDef conf() {
        return new ConfigDef()
                .define(USER_NAME, Type.STRING, Importance.HIGH, USER_NAME_DOC)
                .define(PASSWORD, Type.PASSWORD, Importance.HIGH, PASSWORD_DOC)
                .define(SMTP_HOST, Type.STRING, Importance.HIGH, SMTP_HOST_DOC)
                .define(SMTP_PORT, Type.INT, Importance.HIGH, SMTP_PORT_DOC)
                .define(SMTP_AUTH, Type.BOOLEAN, true, Importance.HIGH, SMTP_AUTH_DOC)
                .define(TLS_ENABLED, Type.BOOLEAN, true, Importance.HIGH, TLS_ENABLED_DOC)
                .define(SMTP_FROM, Type.STRING, Importance.HIGH, SMTP_FROM_DOC)
                .define(SMTP_TO, Type.STRING, Importance.HIGH, SMTP_TO_DOC)
                .define(SMTP_SUBJECT, Type.STRING, Importance.HIGH, SMTP_SUBJECT_DOC)
                .define(SMTP_BODY_TEMPLATE, Type.STRING, Importance.HIGH, SMTP_BODY_TEMPLATE_DOC);
    }

    public String getUserName(){
        return this.getString(USER_NAME);
    }

    public String getSmtpPassword(){
        return this.getPassword(PASSWORD).value();
    }

    public String getSmtpFrom(){
        return this.getString(SMTP_FROM);
    }

    public String getSmtpTo(){
        return this.getString(SMTP_TO);
    }

    public String getSmtpSubject(){
        return this.getString(SMTP_SUBJECT);
    }

    public String getSmtpBodyTemplate(){
        return this.getString(SMTP_BODY_TEMPLATE);
    }

    public Properties getSmtpProps(){
        Properties prop = new Properties();
        prop.put(SMTP_HOST, this.getString(SMTP_HOST));
        prop.put(SMTP_PORT,  this.getInt(SMTP_PORT));
        prop.put(SMTP_AUTH, this.getBoolean(SMTP_AUTH));
        prop.put(TLS_ENABLED, this.getBoolean(TLS_ENABLED));
        return prop;
    }
}
