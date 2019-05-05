package context;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.jms.pool.PooledConnectionFactory;

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

}
