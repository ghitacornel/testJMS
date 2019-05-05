package context;

import javax.jms.*;

public enum MQQueue {

    SINGLE("Single"),
    MANY("Many"),
    MANY_TO_MANY("ManyToMany"),
    MIXED("Mixed");

    private static final int ACKNOWLEDGE_MODE = Session.CLIENT_ACKNOWLEDGE;

    private final String queueName;
    private final Connection connection;

    MQQueue(String queueName) {
        this.queueName = queueName;
        this.connection = Context.prepareConnection();
    }

    public void sendMessage(String message) {
        Session session = null;
        MessageProducer producer = null;
        try {

            session = connection.createSession(false, ACKNOWLEDGE_MODE);
            Destination destination = session.createQueue(queueName);

            producer = session.createProducer(destination);

            TextMessage producerMessage = session.createTextMessage(message);
            producer.send(producerMessage);

        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            Context.close(session, producer);
        }
    }


    public String receiveMessage() {
        Session session = null;
        MessageConsumer consumer = null;
        try {

            session = connection.createSession(false, ACKNOWLEDGE_MODE);
            Destination destination = session.createQueue(queueName);

            consumer = session.createConsumer(destination);
            Message consumerMessage = consumer.receive();

            TextMessage consumerTextMessage = (TextMessage) consumerMessage;

            String text = consumerTextMessage.getText();
            consumerMessage.acknowledge();
            return text;

        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            Context.close(session, consumer);
        }
    }

}
