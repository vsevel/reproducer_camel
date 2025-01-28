package org.acme;

import io.smallrye.common.annotation.Identifier;
import jakarta.inject.Inject;
import jakarta.jms.*;
import jakarta.transaction.SystemException;
import jakarta.transaction.TransactionManager;
import jakarta.transaction.Transactional;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import org.apache.activemq.artemis.jms.client.ActiveMQDestination;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.OnOverflow;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

@Path("/hello")
public class GreetingResource {

    private static final Logger log = LoggerFactory.getLogger(GreetingResource.class);

    private final Queue queue;

    @Inject
    ConnectionFactory connectionFactory;

    @Inject
    TransactionManager transactionManager;

    @Inject
    @Channel("channel-out")
    @OnOverflow(value = OnOverflow.Strategy.UNBOUNDED_BUFFER)
    Emitter<Order> emitter;

    GreetingResource(@ConfigProperty(name="myapp.queue") String queueName) {
        this.queue = ActiveMQDestination.createQueue(queueName);
    }

    @GET
    @Produces(MediaType.TEXT_PLAIN)
    public String hello() {
        return "Hello from Quarkus REST";
    }

    @POST
    @Path("/send-jms")
    @Produces(MediaType.TEXT_PLAIN)
    public String send(@QueryParam("text") @DefaultValue("hello") String text,
                       @QueryParam("count") @DefaultValue("1") int count) throws SystemException {

        sendMessages(text, count, connectionFactory, queue);
        MyBean.init();
        return "OK: sent " + count + " JMS message(s) with text " + text;
    }

    @Transactional
    void sendMessages(String text, int count, ConnectionFactory connectionFactory, Queue q) throws SystemException {

        String tx = ""+transactionManager.getTransaction();
        log.info("Sending " + count + " message(s) with text " + text+" in tx " + tx);
        long start = System.currentTimeMillis();

        try (JMSContext context = connectionFactory.createContext()) {
            JMSProducer producer = context.createProducer();
            for (int i = 0; i < count; i++) {
                TextMessage message = context.createTextMessage(text + "_" + i);
                try {
                    message.setStringProperty("toto", "titi");
                    message.setJMSCorrelationID("mycorr:" + UUID.randomUUID());
                } catch (JMSException e) {
                    throw new RuntimeException(e);
                }
                producer.send(q, message);
            }
        }
        log.info("done sending " + count + " JMS message(s) with text " + text + " in " + (System.currentTimeMillis() - start)
                + " ms");
    }

    @POST
    @Path("/send-kafka")
    @Produces(MediaType.TEXT_PLAIN)
    public String sendKafka(@QueryParam("text") @DefaultValue("hello") String text,
                       @QueryParam("count") @DefaultValue("1") int count) {

        MyBean.init();
        for (int i = 0; i < count; i++) {
            Order order = new Order(i, "X", text + "_" + i);
            emitter.send(order);
        }

        return "OK: sent " + count + " Kafka message(s) with text " + text;
    }

    @POST
    @Path("/jms-to-kafka")
    @Produces(MediaType.TEXT_PLAIN)
    public String jmsToKafka() throws JMSException {
        int count = 0;
        long start = System.currentTimeMillis();
        try (JMSContext context = connectionFactory.createContext(Session.CLIENT_ACKNOWLEDGE)) {
            JMSConsumer consumer = context.createConsumer(queue);
            while (true) {
                TextMessage message = (TextMessage) consumer.receiveNoWait();
                if (message == null) {
                    break;
                }
                Order order = new Order(count, "X", message.getText());
                emitter.send(order);
                count++;
                message.acknowledge();
            }
        }
        long delta = System.currentTimeMillis() - start;
        String rate = count == 0 ? "N/A" :  ""+((int)(count * 1000.0 / delta));
        return "OK: sent "+count+" JMS message(s) to Kafka in "+delta+" ms ("+rate+" messages/s)";
    }
}
