package context;

import javax.jms.*;

public enum MQQueue {

    SINGLE("Single"),
    MANY("Many"),
    MANY_TO_MANY("ManyToMany"),
    MIXED("Mixed");

    private static final int ACKNOWLEDGE_MODE = Session.CLIENT_ACKNOWLEDGE;

    private final String queueName;

    MQQueue(String queueName) {
        this.queueName = queueName;
    }

    public void sendMessage(String message) {
        Connection connection = null;
        Session session = null;
        MessageProducer producer = null;
        try {

            connection = prepareConnection();

            session = connection.createSession(false, ACKNOWLEDGE_MODE);
            Destination destination = session.createQueue(queueName);

            producer = session.createProducer(destination);

            TextMessage producerMessage = session.createTextMessage(message);
            producer.send(producerMessage);

        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            close(connection, session, producer);
        }
    }

    private static void close(Connection connection, Session session, MessageProducer producer) {
        try {
            if (producer != null) {
                producer.close();
            }
            if (session != null) {
                session.close();
            }
            if (connection != null) {
                connection.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public String receiveMessage() {
        Connection connection = null;
        Session session = null;
        MessageConsumer consumer = null;
        try {

            connection = prepareConnection();

            session = connection.createSession(false, ACKNOWLEDGE_MODE);
            Destination destination = session.createQueue(queueName);

            consumer = session.createConsumer(destination);
            Message consumerMessage = consumer.receive();

            TextMessage consumerTextMessage = (TextMessage) consumerMessage;

            String text = consumerTextMessage.getText();
            consumerMessage.acknowledge();
            if (text == null) System.err.println("null text");// TODO
            return text;

        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            close(connection, session, consumer);
        }
    }

    private static void close(Connection connection, Session session, MessageConsumer consumer) {
        try {
            if (consumer != null) {
                consumer.close();
            }
            if (session != null) {
                session.close();
            }
            if (connection != null) {
                connection.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static Connection prepareConnection() {
        try {

            Connection connection = Context.pooledConnectionFactory.createConnection();
            connection.start();
            return connection;
        } catch (Exception e) {
            throw new RuntimeException("error creating connection", e);
        }
    }
}
