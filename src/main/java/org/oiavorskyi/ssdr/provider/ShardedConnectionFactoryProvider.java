package org.oiavorskyi.ssdr.provider;

import org.oiavorskyi.ssdr.specification.RedisShardSpec;
import org.oiavorskyi.ssdr.shardingstrategy.RedisShardSpecRegistry;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.util.Assert;

public class ShardedConnectionFactoryProvider implements InitializingBean {
    private RedisShardSpecRegistry specProvider;
    private RedisConnectionFactory[] factories;

    public ShardedConnectionFactoryProvider(RedisShardSpecRegistry specProvider) {
        this.specProvider = specProvider;
    }

    public RedisConnectionFactory getConnectionFactory(int shardId) {
        Assert.isTrue(shardId >= 0 && shardId < specProvider.getShardsNumber(),
                "shardId should be between 0 and " + specProvider.getShardsNumber());
        Assert.notNull(factories, "ensure that afterPropertiesSet method was called prior to " +
                "using this method");

        return factories[shardId];
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        Assert.notNull(specProvider, "specProvider should be set prior to bean usage");
        Assert.isTrue(specProvider.getShardsNumber() > 0, "shards count should be at least 1");

        factories = new RedisConnectionFactory[specProvider.getShardsNumber()];

        for (int i = 0; i < specProvider.getShardsNumber(); i++) {
            RedisShardSpec shardSpec = specProvider.getShardSpecById(i);
            factories[i] = createConnectionFactory(shardSpec);
        }
    }

    public RedisConnectionFactory createConnectionFactory(RedisShardSpec shardSpec) {
        JedisConnectionFactory factory = new JedisConnectionFactory();
        factory.setHostName(shardSpec.getHost());
        factory.setPort(shardSpec.getPort());
        factory.setDatabase(shardSpec.getDb());
        factory.afterPropertiesSet();
        return factory;
    }

}
