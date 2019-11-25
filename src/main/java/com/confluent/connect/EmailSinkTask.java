package com.confluent.connect;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateException;

import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.LoggerFactory;

import javax.mail.*;
import javax.mail.internet.AddressException;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import java.io.*;
import java.util.*;
import java.util.stream.Collectors;


public class EmailSinkTask extends SinkTask {
    private static final org.slf4j.Logger log = LoggerFactory.getLogger(EmailSinkTask.class);
    EmailSinkConnectorConfig emailSinkConnectorConfig;
    ObjectMapper objectMapper = new ObjectMapper();
    @Override
    public String version() {
        return VersionUtil.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        this.emailSinkConnectorConfig = new EmailSinkConnectorConfig(props);
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        log.info("got records "+records.size());



            Configuration cfg = new Configuration();
            try {
                if(records.size()>0) {
                    List<EmailObject> values = records.stream().map(sinkRecord -> {
                        try {
                            EmailObject emailObject = new EmailObject();

                            for (Header header: sinkRecord.headers()) {
                                emailObject.addHeader(header.key(), header.value().toString());

                            }

                            if(sinkRecord.key()!=null)
                                emailObject.setKey(sinkRecord.key().toString());

                            emailObject.setValue(objectMapper.readValue( sinkRecord.value().toString(), JsonNode.class));
                            return emailObject;
                        } catch (JsonProcessingException e) {
                            return null;
                        }

                    }).filter(value->value!=null).collect(Collectors.toList());

                    Template temp = new Template("email-template", new StringReader(this.emailSinkConnectorConfig.getSmtpBodyTemplate()),
                            cfg);
                    Map<String, Object> templateVars = new HashMap<>();
                    templateVars.put("records", values);


                    StringWriter stringWriter = new StringWriter();
                    //Writer out = new OutputStreamWriter(System.out);
                    temp.process(templateVars, stringWriter);
                    String emailBody = stringWriter.toString();
                    final String username = this.emailSinkConnectorConfig.getUserName();
                    final String password = this.emailSinkConnectorConfig.getSmtpPassword();
                    Session session = Session.getInstance(this.emailSinkConnectorConfig.getSmtpProps(),
                            new javax.mail.Authenticator() {
                                protected PasswordAuthentication getPasswordAuthentication() {
                                    return new PasswordAuthentication(username, password);
                                }
                            });


                    Message message = new MimeMessage(session);
                    message.setFrom(new InternetAddress(this.emailSinkConnectorConfig.getSmtpFrom()));
                    message.setRecipients(
                            Message.RecipientType.TO,
                            InternetAddress.parse(this.emailSinkConnectorConfig.getSmtpTo())
                    );
                    message.setSubject(this.emailSinkConnectorConfig.getSmtpSubject());
                    log.info("sending email with body " + emailBody);
                    message.setText(emailBody);

                    Transport.send(message);
                }

            } catch (IOException | TemplateException |  MessagingException  e) {
                log.error(e.getMessage());
            }


    }

    @Override
    public void stop() {

    }
}
