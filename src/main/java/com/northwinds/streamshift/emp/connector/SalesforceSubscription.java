package com.northwinds.streamshift.emp.connector;

public interface SalesforceSubscription {

    /**
     * Cancel the subscription
     */
    void cancel();

    /**
     * @return the current replayFrom event id of the subscription
     */
    long getReplayFrom();

    /**
     * @return the topic subscribed to
     */
    String getTopic();

}
