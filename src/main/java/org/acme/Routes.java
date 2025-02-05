package org.acme;

import io.quarkiverse.messaginghub.pooled.jms.JmsPoolLocalTransactionConnectionFactory;
import io.quarkus.arc.Arc;
import jakarta.jms.QueueConnectionFactory;
import jakarta.jms.Session;
import jakarta.transaction.TransactionManager;
import org.apache.activemq.artemis.jms.client.ActiveMQQueueConnectionFactory;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.kafka.consumer.KafkaManualCommit;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;

import java.util.List;

import static org.apache.camel.builder.endpoint.StaticEndpointBuilders.sjms2;

public class Routes extends RouteBuilder {

    @Override
    public void configure() throws Exception {

        Config config = ConfigProvider.getConfig();
        String queue = config.getConfigValue("myapp.queue").getValue();
        String topic = config.getConfigValue("myapp.topic").getValue();

//        String scenario = "jms"; // send 100000; dev service avg=23000; LO=25000
//        String scenario = "jms-to-kafka"; // send 100000; dev service avg=3200; LO=170
//        String scenario = "jms-to-kafka-tx"; // send 10000; dev service avg=97; LO=110
//        String scenario = "kafka"; // send 1000000; dev service avg=38000; LO=25000
//        String scenario = "kafka-to-jms"; // send 1000; dev service avg=120; LO=125
//        String scenario = "kafka-to-jms-manual-commit"; // send 1000; dev service avg=95; LO=30
        String scenario = "sjms-to-kafka-with-tx";
//        String scenario = "noop";

        String sc = config.getOptionalValue("scenario", String.class).orElse(scenario);
        log.info("running scenario " + sc);

        // dynamic
        String uri = config.getValue("quarkus.artemis.url", String.class);
        String user = config.getValue("quarkus.artemis.username", String.class);
        String password = config.getValue("quarkus.artemis.password", String.class);
        var cf = new ActiveMQQueueConnectionFactory(uri, user, password);
        var tm = Arc.container().instance(TransactionManager.class).get();
        var poolCF = new JmsPoolLocalTransactionConnectionFactory();
        poolCF.setTransactionManager(tm);
        poolCF.setConnectionFactory(cf);
        getContext().getRegistry().bind("dynamic-cf", poolCF);

        if (sc.equals("jms")) {

            // curl -X POST localhost:18080/hello/send-jms?count=1000
            from("jms:queue:" + queue + "?concurrentConsumers=10")
                    .bean("my-bean", "fromJMS");

        } else if (sc.equals("dynamic-cf")) {

            // curl -X POST localhost:18080/hello/send-jms?count=1000
            from(sjms2("queue:" + queue + "?concurrentConsumers=10").connectionFactory("#dynamic-cf"))
                    .bean("my-bean", "fromJMS");

        } else if (sc.equals("jms-to-kafka")) {

            // curl -X POST localhost:18080/hello/send-jms?count=1000
            from("jms:queue:" + queue + "?concurrentConsumers=10")
                    .bean("my-bean", "fromJMS")
                    .to("kafka:" + topic);

            from("kafka:" + topic)
                    .bean("my-bean", "fromKafka");

        } else if (sc.equals("jms-to-kafka-tx")) {

            // curl -X POST localhost:18080/hello/send-jms?count=1000
            from("jms:queue:" + queue + "?concurrentConsumers=10&transacted=true")
                    .bean("my-bean", "fromJMS")
                    .to("kafka:" + topic);

            from("kafka:" + topic)
                    .bean("my-bean", "fromKafka");

        } else if (sc.equals("sjms-to-kafka-tx")) {

            // curl -X POST localhost:18080/hello/send-jms?count=1000
            from("sjms2:queue:" + queue + "?concurrentConsumers=10&transacted=true")
                    .bean("my-bean", "fromJMS")
                    .to("kafka:" + topic);

//            from("kafka:"+topic)
//                    .bean("my-bean", "fromKafka");

        } else if (sc.equals("kafka-to-jms")) {

            // curl -X POST localhost:18080/hello/send-kafka?count=1000
            from("kafka:" + topic)
                    .bean("my-bean", "fromKafka")
                    .to("jms:queue:" + queue);

            from("jms:queue:" + queue + "?concurrentConsumers=10")
                    .bean("my-bean", "fromJMS");

        } else if (sc.equals("kafka-to-jms-manual-commit")) {

            // curl -X POST localhost:18080/hello/send-kafka?count=1000
            from("kafka:" + topic + "?consumersCount=4&allowManualCommit=true&autoCommitEnable=false")
                    .bean("my-bean", "fromKafka")
                    .to("sjms2:queue:" + queue)
                    .onCompletion().onCompleteOnly().process(new Processor() {
                        @Override
                        public void process(Exchange exchange) throws Exception {
                            KafkaManualCommit manualCommit = exchange.getIn().getHeader("CamelKafkaManualCommit", KafkaManualCommit.class);
                            manualCommit.commit();
                        }
                    });

//            from("jms:queue:"+queue+"?concurrentConsumers=10")
//                    .bean("my-bean", "fromJMS");

        } else if (sc.equals("kafka-to-jms-manual-commit-batch")) {

            // curl -X POST localhost:18080/hello/send-kafka?count=1000
            from("kafka:" + topic + "?consumersCount=4&allowManualCommit=true&autoCommitEnable=false&batching=true&maxPollRecords=500")
                    .onCompletion().onCompleteOnly().process(new Processor() {
                        @Override
                        public void process(Exchange exchange) throws Exception {
                            List<Exchange> exchanges = exchange.getIn().getBody(List.class);
                            Exchange last = exchanges.get(exchanges.size() - 1);
                            KafkaManualCommit manualCommit = last.getIn().getHeader("CamelKafkaManualCommit", KafkaManualCommit.class);
                            manualCommit.commit();
                        }
                    }).end()
                    .bean("my-bean", "fromKafkaMulti")
                    .split(method(new MyCustomSplitter(), "splitMe"))
                    .to("sjms2:queue:" + queue + "?transacted=true");

        } else if (sc.equals("kafka")) {

            // curl -X POST localhost:18080/hello/send-kafka?count=1000
            from("kafka:" + topic)
                    .bean("my-bean", "fromKafka");

        } else if (sc.equals("sjms-to-kafka-with-tx")) {

            from("sjms2:queue:" + queue + "?concurrentConsumers=10&transacted=true")
                    .bean("my-bean", "fromJMS")
                    .to("kafka:" + topic)
                    .onCompletion().onFailureOnly().process(new Processor() {
                        @Override
                        public void process(Exchange exchange) throws Exception {
                            Object camelJMSSession = exchange.getProperties().get("CamelJMSSession");
                            if (camelJMSSession instanceof Session session) {
                                session.rollback();
                            }
                        }
                    });

        } else if (sc.equals("sjms-to-kafka-with-ack")) {

            from("sjms2:queue:" + queue + "?concurrentConsumers=10&acknowledgementMode=CLIENT_ACKNOWLEDGE")
                    .bean("my-bean", "fromJMS")
                    .to("kafka:" + topic)
                    .onCompletion().onFailureOnly().process(new Processor() {
                        @Override
                        public void process(Exchange exchange) throws Exception {
                            Object camelJMSSession = exchange.getProperties().get("CamelJMSSession");
                            if (camelJMSSession instanceof Session session) {
                                session.recover();
                            }
                        }
                    });


        } else {
            log.info("no scenario " + sc);
        }
    }
}