package org.oiavorskyi.ssdr.jedis;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.oiavorskyi.ssdr.RedisShardSpec;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.jedis.JedisConnection;
import org.springframework.data.redis.connection.jedis.JedisConverters;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisSentinelPool;
import redis.clients.jedis.Protocol;

import java.util.Set;

/**
 * Implementation of Jedis factory based on Sentinel Pool
 */
public class ResilientJedisConnectionFactory implements InitializingBean, DisposableBean,
        RedisConnectionFactory {

    private final static Log logger = LogFactory.getLog(ResilientJedisConnectionFactory.class);

    private JedisSentinelPool jedisSentinelPool;
    private String masterHostName;
    private Set<String> sentinels;
    private int timeout = Protocol.DEFAULT_TIMEOUT;
    private String password;
    private int databaseIndex = Protocol.DEFAULT_DATABASE;
    private boolean convertPipelineAndTxResults = true;


    public ResilientJedisConnectionFactory(RedisShardSpec redisShardSpec){
        this(redisShardSpec.getMasterName(), redisShardSpec.getSentinels(), redisShardSpec.getDb());
    }

    public ResilientJedisConnectionFactory(String masterHostName, Set<String> sentinels) {
        this(masterHostName, sentinels, null);
    }

    public ResilientJedisConnectionFactory(String masterHostName, Set<String> sentinels, String password) {
        this(masterHostName, sentinels, password, Protocol.DEFAULT_DATABASE);
    }

    public ResilientJedisConnectionFactory(String masterHostName, Set<String> sentinels, int databaseIndex) {
        this(masterHostName, sentinels, Protocol.DEFAULT_TIMEOUT, null, databaseIndex);
    }

    public ResilientJedisConnectionFactory(String masterHostName, Set<String> sentinels, int timeout, String password) {
        this(masterHostName, sentinels, timeout, password, Protocol.DEFAULT_DATABASE);
    }

    public ResilientJedisConnectionFactory(String masterHostName, Set<String> sentinels, String password, int databaseIndex) {
        this(masterHostName, sentinels, Protocol.DEFAULT_TIMEOUT, password, databaseIndex);
    }

    public ResilientJedisConnectionFactory(String masterHostName, Set<String> sentinels, int timeout,
                                           String password, int databaseIndex) {
        this.masterHostName = masterHostName;
        this.sentinels = sentinels;
        this.timeout = timeout;
        this.password = password;
        this.databaseIndex = databaseIndex;
    }

    @Override
    public void afterPropertiesSet() {

        jedisSentinelPool = new JedisSentinelPool(masterHostName, sentinels, new GenericObjectPoolConfig(),
                timeout, password, databaseIndex);

    }

    @Override
    public RedisConnection getConnection() {
        Jedis jedis = jedisSentinelPool.getResource();
        JedisConnection connection = new JedisConnection(jedis, jedisSentinelPool, databaseIndex);
        connection.setConvertPipelineAndTxResults(convertPipelineAndTxResults);

        return connection;
    }

    public void recycleConnection(RedisConnection connection){
        connection.close();
    }

    @Override
    public boolean getConvertPipelineAndTxResults() {
        return convertPipelineAndTxResults;
    }

    @Override
    public DataAccessException translateExceptionIfPossible(RuntimeException e) {
        return JedisConverters.toDataAccessException(e);
    }

    @Override
    public void destroy() throws Exception {
        try {
            jedisSentinelPool.destroy();
        } catch (Exception ex) {
            logger.warn("Cannot properly close JedisSentinelPool", ex);
        }
        jedisSentinelPool = null;
    }

    public void setConvertPipelineAndTxResults(boolean convertPipelineAndTxResults) {
        this.convertPipelineAndTxResults = convertPipelineAndTxResults;
    }

    // For testing purpose
    public JedisSentinelPool getJedisSentinelPool(){
        return jedisSentinelPool;
    }

}
