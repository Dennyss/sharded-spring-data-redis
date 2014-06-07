package org.oiavorskyi.ssdr.jedis;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;

/**
 * Implementation of Jedis factory based on Sentinel Pool
 */
public class ResilientJedisConnectionFactory implements InitializingBean, DisposableBean,
        RedisConnectionFactory {

    @Override
    public void afterPropertiesSet() {

    }

    @Override
    public RedisConnection getConnection() {
        return null;
    }

    @Override
    public boolean getConvertPipelineAndTxResults() {
        return false;
    }

    @Override
    public DataAccessException translateExceptionIfPossible( RuntimeException e ) {
        return null;
    }

    @Override
    public void destroy() throws Exception {

    }
}
