package com.confluent.connect;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EmailSinkConnector  extends SinkConnector {
    private static final Logger log = LoggerFactory.getLogger(EmailSinkConnector.class);
    private Map<String, String> configProps;

    @Override
    public void start(Map<String, String> props) {
        log.info("Starting connector.", props);
        try {
            configProps = props;
            new EmailSinkConnectorConfig(props);
        } catch (ConfigException e) {
            throw new ConnectException("Couldn't start EmailSinkConnector due to configuration error",e);
        }
    }

    @Override
    public Class<? extends Task> taskClass() {
        return EmailSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        log.info("Setting task configurations for {} workers.", maxTasks);
        final List<Map<String, String>> configs = new ArrayList<>(maxTasks);
        for (int i = 0; i < maxTasks; ++i) {
            configs.add(configProps);
        }
        return configs;
    }

    @Override
    public void stop() {

    }

    @Override
    public ConfigDef config() {
        return EmailSinkConnectorConfig.conf();
    }

    @Override
    public String version() {
        return VersionUtil.getVersion();
    }
}
