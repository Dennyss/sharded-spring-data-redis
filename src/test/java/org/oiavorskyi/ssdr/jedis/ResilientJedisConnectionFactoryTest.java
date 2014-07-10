package org.oiavorskyi.ssdr.jedis;

import org.junit.Test;
import org.oiavorskyi.ssdr.utils.TestUtil;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.jedis.JedisConnection;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisSentinelPool;

import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Created by Denys Kovalenko on 7/9/2014.
 */

/**
 * For running this test, redis master/slave and sentinel must be up and running.
 * My configuration is:
 * - master:   172.17.34.126:6379
 * - slave:    172.17.34.126:6380
 * - sentinel: 172.17.34.126:6381
 */
public class ResilientJedisConnectionFactoryTest {

    @Test
    public void shouldSaveAndReadSomeSimpleData() throws Exception {
        RedisConnectionFactory connectionFactory = createConnectionFactory();
        RedisConnection connection = connectionFactory.getConnection();
        connection.set("key".getBytes(), "value".getBytes());

        assertEquals("value", new String(connectionFactory.getConnection().get("key".getBytes())));
    }

    @Test
    public void shouldReturnConnectionBackToThePool() throws Exception {
        ResilientJedisConnectionFactory connectionFactory = createConnectionFactory();
        RedisConnection connectionToBeReturned = connectionFactory.getConnection();
        connectionToBeReturned.set("testKey".getBytes(), "testValue".getBytes());
        // Return connection back
        connectionFactory.recycleConnection(connectionToBeReturned);

        RedisConnection newConnection = connectionFactory.getConnection();

        // The old(returned) connection and the new connection have the same resource
        assertTrue(extractResource(connectionToBeReturned) == extractResource(newConnection));
    }

    @Test
    public void shouldNotReturnConnectionBackToThePool() throws Exception {
        ResilientJedisConnectionFactory connectionFactory = createConnectionFactory();
        RedisConnection connectionToBeReturned = connectionFactory.getConnection();

        RedisConnection newConnection = connectionFactory.getConnection();
        // The old(returned) connection and the new connection have different resources,
        // because old connection was not utilized and for new connection was created new resource
        assertFalse(extractResource(connectionToBeReturned) == extractResource(newConnection));
    }

    @Test
    public void shouldNewConnectionWorkProperlyAfterReturningPrevious() throws Exception {
        ResilientJedisConnectionFactory connectionFactory = createConnectionFactory();
        RedisConnection connectionToBeReturned = connectionFactory.getConnection();

        connectionToBeReturned.set("testKey".getBytes(), "testValue".getBytes());
        // Return connection back
        connectionFactory.recycleConnection(connectionToBeReturned);

        RedisConnection newConnection = connectionFactory.getConnection();
        // Check that new connection is able read the data populated by previous connection
        assertEquals("testValue", new String(newConnection.get("testKey".getBytes())));
    }

    @Test
    public void shouldNotLostDataInCaseOfFailover() throws InterruptedException {
        ResilientJedisConnectionFactory connectionFactory = createConnectionFactory();

        // Save some data
        connectionFactory.getConnection().set("dataKey".getBytes(), "dataValue".getBytes());

        forceFailover(connectionFactory);

        // Check that after failover it's possible to read previously populated data
        assertEquals("PONG", connectionFactory.getConnection().ping());
        assertEquals("dataValue", new String(connectionFactory.getConnection().get("dataKey".getBytes())));
    }

    private void forceFailover(ResilientJedisConnectionFactory connectionFactory)
            throws InterruptedException {

        // Configuring sentinel for force failover
        Jedis sentinel = new Jedis("172.17.34.126", 6381);
        sentinel.sentinelFailover("masterDB");

        waitForFailover(connectionFactory.getJedisSentinelPool(), sentinel);
    }

    private void waitForFailover(JedisSentinelPool pool, Jedis sentinel) throws InterruptedException {
        HostAndPort newMaster = TestUtil.waitForNewPromotedMaster(sentinel);
        waitWhileJedisSentinelPoolRecognizesNewMaster(pool, newMaster);
    }

    private void waitWhileJedisSentinelPoolRecognizesNewMaster(JedisSentinelPool pool, HostAndPort newMaster) throws InterruptedException {
        while (true) {
            String host = pool.getCurrentHostMaster().getHost();
            int port = pool.getCurrentHostMaster().getPort();

            if (host.equals(newMaster.getHost()) && port == newMaster.getPort())
                break;

            // JedisSentinelPool's master is not yet changed, sleep...
            Thread.sleep(100);
        }
    }

    private ResilientJedisConnectionFactory createConnectionFactory() {
        Set<String> sentinels = new HashSet<>();
        sentinels.add("172.17.34.126:6381");

        String masterName = "masterDB";
        String password = "123";
        ResilientJedisConnectionFactory connectionFactory = new ResilientJedisConnectionFactory(masterName, sentinels, password, 1);
        connectionFactory.afterPropertiesSet();

        return connectionFactory;
    }

    private Jedis extractResource(RedisConnection redisConnection) throws Exception {
        JedisConnection connection = (JedisConnection) redisConnection;
        Field field = JedisConnection.class.getDeclaredField("jedis");
        field.setAccessible(true);
        return (Jedis) field.get(connection);
    }

}
