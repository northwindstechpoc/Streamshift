package com.northwinds.streamshift;

import static com.northwinds.streamshift.emp.LoginHelper.login;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import io.dropwizard.Application;
import io.dropwizard.assets.AssetsBundle;
import io.dropwizard.configuration.EnvironmentVariableSubstitutor;
import io.dropwizard.configuration.SubstitutingSourceProvider;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import org.glassfish.jersey.linking.DeclarativeLinkingFeature;

import org.eclipse.jetty.util.ajax.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.northwinds.streamshift.emp.BayeuxParameters;
import com.northwinds.streamshift.emp.EmpConnector;
import com.northwinds.streamshift.emp.SalesforceSubscription;
import com.northwinds.streamshift.emp.KafkaProducer;
import com.northwinds.streamshift.emp.KafkaConfig;

import com.loginbox.heroku.config.HerokuConfiguration;

public class Streamshift {
	
    private static Logger LOG = LoggerFactory.getLogger(Streamshift.class);
    
    public static void main(String[] argv) throws Exception {
        KafkaConfig kafkaConfig = new KafkaConfig();
        KafkaProducer producer = new KafkaProducer(kafkaConfig);
        
        producer.start();

        if (null == System.getenv("SF_USER") || null == System.getenv("SF_PASS") || null == System.getenv("SF_TOPIC")) {
            LOG.error("Usage: Set SF_USER, SF_PASS and SF_TOPIC as environment variables to run");
            System.exit(1);
        } 
    	
        long replayFrom = EmpConnector.REPLAY_FROM_EARLIEST;
        if (null != System.getenv("SF_REPLAY_FROM")) {
            replayFrom = Long.parseLong(System.getenv("SF_REPLAY_FROM"));
        }

        BearerTokenProvider tokenProvider = new BearerTokenProvider(() -> {
            try {
                return login(System.getenv("SF_USER"), System.getenv("SF_PASS"));
            } catch (Exception e) {
            	LOG.error(e.getMessage());
                e.printStackTrace(System.err);
                System.exit(1);
                throw new RuntimeException(e);
            }
        });

        BayeuxParameters params = tokenProvider.login();

        Consumer<Map<String, Object>> consumer = event -> producer.send(JSON.toString(event));

        EmpConnector connector = new EmpConnector(params);

        connector.setBearerTokenProvider(tokenProvider);

        connector.start().get(5, TimeUnit.SECONDS);

        SalesforceSubscription subscription = connector.subscribe(System.getenv("SF_TOPIC"), replayFrom, consumer).get(5, TimeUnit.SECONDS);

        LOG.info(String.format("Subscribed: %s", subscription));
    }
}
