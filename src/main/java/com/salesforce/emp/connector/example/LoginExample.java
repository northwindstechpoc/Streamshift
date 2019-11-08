/*
 * Copyright (c) 2016, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.TXT file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.emp.connector.example;

import com.salesforce.emp.connector.*;
import static com.salesforce.emp.connector.LoginHelper.login;

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

import com.salesforce.emp.connector.BayeuxParameters;
import com.salesforce.emp.connector.EmpConnector;
import com.salesforce.emp.connector.TopicSubscription;
import com.loginbox.heroku.config.HerokuConfiguration;

/**
 * An example of using the EMP connector using login credentials
 *
 * @author hal.hildebrand
 * @since API v37.0
 * 
 * updated for Heroku deploy troy.sellers
 * 
 */
public class LoginExample {
	
    private static Logger LOG = LoggerFactory.getLogger(LoginExample.class);
    private static Environment env;
    private final KafkaConfig kafkaConfig = new KafkaConfig();
    private static DemoProducer producer = new DemoProducer(kafkaConfig.getKafkaConfig());
    
    public static void main(String[] argv) throws Exception {
        
        env.lifecycle().manage(producer);
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

        //Consumer<Map<String, Object>> consumer = event -> LOG.info(String.format("Received:\n%s", JSON.toString(event)));
        Consumer<Map<String, Object>> consumer = event -> producer.send(JSON.toString(event));

        EmpConnector connector = new EmpConnector(params);

        connector.setBearerTokenProvider(tokenProvider);

        connector.start().get(5, TimeUnit.SECONDS);

        TopicSubscription subscription = connector.subscribe(System.getenv("SF_TOPIC"), replayFrom, consumer).get(5, TimeUnit.SECONDS);

        LOG.info(String.format("Subscribed: %s", subscription));
    }
}
