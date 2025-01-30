package org.acme;

import io.quarkiverse.ironjacamar.ResourceEndpoint;
import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageListener;
import jakarta.jms.TextMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// @ApplicationScoped
// @ResourceEndpoint(activationSpecConfigKey = "myqueue")
// @Identifier("CFRA")
public class MyJMSListener implements MessageListener {

    Logger log = LoggerFactory.getLogger(MyJMSListener.class);

    @Override
    public void onMessage(Message message) {
        try {
            log.info("received message " + ((TextMessage) message).getText());
        } catch (JMSException e) {
            throw new RuntimeException(e);
        }
    }
}
