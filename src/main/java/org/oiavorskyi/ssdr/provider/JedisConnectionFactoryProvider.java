package org.oiavorskyi.ssdr.provider;

import org.oiavorskyi.ssdr.specification.RedisSentinelSpec;
import org.oiavorskyi.ssdr.jedis.ResilientJedisConnectionFactory;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.util.Assert;

public class JedisConnectionFactoryProvider {
    private RedisSentinelSpec redisSentinelSpec;

    public JedisConnectionFactoryProvider(RedisSentinelSpec redisSentinelSpec) {
        this.redisSentinelSpec = redisSentinelSpec;
    }

    public RedisConnectionFactory createConnectionFactory() {
        ResilientJedisConnectionFactory factory = new ResilientJedisConnectionFactory(redisSentinelSpec);
        factory.afterPropertiesSet();
        return factory;
    }

    public void afterPropertiesSet() throws Exception {
        Assert.notNull(redisSentinelSpec, "redisSentinelSpec should be set prior to bean usage");
        Assert.isTrue(redisSentinelSpec.getSentinels().size() > 0, "sentinels count should be at least 1");
    }
}
