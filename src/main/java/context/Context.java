package context;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.jms.pool.PooledConnectionFactory;

import jakarta.jms.Connection;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;

class Context {

    private static final ActiveMQConnectionFactory connectionFactory;
    static final PooledConnectionFactory pooledConnectionFactory;

    static {

        connectionFactory = new ActiveMQConnectionFactory();

        pooledConnectionFactory = new PooledConnectionFactory();
        pooledConnectionFactory.setConnectionFactory(connectionFactory);
        pooledConnectionFactory.setMaxConnections(10);
        pooledConnectionFactory.setCreateConnectionOnStartup(true);

        pooledConnectionFactory.setReconnectOnException(true);
    }

    public static Connection prepareConnection() {
        try {
            Connection connection = Context.pooledConnectionFactory.createConnection();
            connection.start();
            return connection;
        } catch (Exception e) {
            throw new RuntimeException("error creating connection", e);
        }
    }

    public static void close(Session session, MessageProducer producer) {
        close(session, producer, null);
    }

    public static void close(Session session, MessageConsumer consumer) {
        close(session, null, consumer);
    }

    private static void close(Session session, MessageProducer producer, MessageConsumer consumer) {
        try {
            if (consumer != null) {
                consumer.close();
            }
            if (producer != null) {
                producer.close();
            }
            if (session != null) {
                session.close();
            }
        } catch (Exception e) {
            throw new RuntimeException("error closing resources", e);
        }
    }

}
