package org.oiavorskyi.ssdr.jedis;

import org.junit.Test;
import org.oiavorskyi.ssdr.common.PropertyKeys;
import org.oiavorskyi.ssdr.common.PropertyLoader;
import org.springframework.data.redis.connection.RedisConnection;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;

/**
 * Created by Denys Kovalenko on 8/29/2014.
 */
public class WritingReadingTest {

    @Test
    public void shouldReadAllDataWereWritten() {

        ResilientJedisConnectionFactory connectionFactory = createConnectionFactory();

        for (int k = 0; k < 10; k++) {

            // Writing data
            for (int i = 0; i < 10; i++) {
                RedisConnection redisConnection = getConnection(connectionFactory);
                String key = "key" + i;
                String value = "value" + i;

                try {
                    redisConnection.set(key.getBytes(), value.getBytes());
                } catch (Exception e) {
                    // Retry
                    redisConnection = getConnection(connectionFactory);
                    redisConnection.set(key.getBytes(), value.getBytes());
                } finally {
                    returnConnection(connectionFactory, redisConnection);
                }

                System.out.println("Sent to DB. Key: " + key + ". Value: " + value);
                pause(500);  // half second
            }

            // Reading data
            for (int i = 0; i < 10; i++) {
                RedisConnection redisConnection = getConnection(connectionFactory);
                String key = "key" + i;
                String value = null;

                try {
                    value = new String(redisConnection.get(("key" + i).getBytes()));
                } catch (Exception e) {
                    // Retry
                    redisConnection = getConnection(connectionFactory);
                    value = new String(redisConnection.get(("key" + i).getBytes()));
                } finally {
                    returnConnection(connectionFactory, redisConnection);
                }

                System.out.println("Reading from DB. Key: " + key + ". Value: " + value);
                assertEquals("value" + i, value);
                pause(500);  // half second
            }

        }
    }

    private void returnConnection(ResilientJedisConnectionFactory connectionFactory, RedisConnection redisConnection) {
        try {
            connectionFactory.recycleConnection(redisConnection);
        } catch (Exception e) {
            System.out.println("Exception in closing connection 2");
        }
    }

    private ResilientJedisConnectionFactory createConnectionFactory() {
        Set<String> sentinels = new HashSet<>();
        sentinels.add(PropertyLoader.getSentinel1HostPort());
        //sentinels.add(PropertyLoader.getSentinel2HostPort());

        String masterName = PropertyLoader.getProperty(PropertyKeys.REDIS_MASTER);
        String password = PropertyLoader.getProperty(PropertyKeys.REDIS_PASSWORD);
        ResilientJedisConnectionFactory connectionFactory = new ResilientJedisConnectionFactory(masterName, sentinels, password, 1);
        connectionFactory.afterPropertiesSet();

        return connectionFactory;
    }

    private void pause(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private RedisConnection getConnection(ResilientJedisConnectionFactory connectionFactory) {
        RedisConnection redisConnection = null;

        for (; ; ) {
            try {
                redisConnection = connectionFactory.getConnection();
                break;
            } catch (Exception e) {
                System.out.println("Connection lost. Reconnecting after 1 sec ...");
                pause(1000);  // One second
            }
        }

        return redisConnection;
    }

}
