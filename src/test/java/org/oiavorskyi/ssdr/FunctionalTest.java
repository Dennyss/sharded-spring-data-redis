package org.oiavorskyi.ssdr;

import org.junit.Test;
import org.oiavorskyi.ssdr.common.PropertyKeys;
import org.oiavorskyi.ssdr.common.PropertyLoader;
import org.oiavorskyi.ssdr.provider.JedisConnectionFactoryProvider;
import org.oiavorskyi.ssdr.provider.ShardedConnectionFactoryProvider;
import org.oiavorskyi.ssdr.proxy.RedisTemplateProxy;
import org.oiavorskyi.ssdr.proxy.ShardedRedisTemplateProxy;
import org.oiavorskyi.ssdr.shardingstrategy.RedisShardingStrategy;
import org.oiavorskyi.ssdr.specification.DefaultRedisShardSpecRegistry;
import org.oiavorskyi.ssdr.specification.RedisSentinelSpec;
import org.oiavorskyi.ssdr.specification.RedisShardSpec;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

import static org.junit.Assert.assertEquals;

public class FunctionalTest {

    @Test
    public void showcaseShardingUsage() throws Exception {
        final int NUM_SHARDS = 4;
        RedisShardingStrategy<String> modStrategy = new RedisShardingStrategy<String>() {
            @Override
            public int getShardsCount() {
                return NUM_SHARDS;
            }

            @Override
            public int getShardIdByKey( String key ) {
                return key.hashCode() % NUM_SHARDS;
            }
        };

        DefaultRedisShardSpecRegistry specProvider = new DefaultRedisShardSpecRegistry();

        specProvider.addSpecForShard(0, RedisShardSpec.fromHostAndPort(PropertyLoader.getProperty(PropertyKeys.REDIS_LOCAL_HOST), PropertyLoader.getLocalPort(), 0));
        specProvider.addSpecForShard(1, RedisShardSpec.fromHostAndPort(PropertyLoader.getProperty(PropertyKeys.REDIS_LOCAL_HOST), PropertyLoader.getLocalPort(), 1));
        specProvider.addSpecForShard(2, RedisShardSpec.fromHostAndPort(PropertyLoader.getProperty(PropertyKeys.REDIS_LOCAL_HOST), PropertyLoader.getLocalPort(), 2));
        specProvider.addSpecForShard(3, RedisShardSpec.fromHostAndPort(PropertyLoader.getProperty(PropertyKeys.REDIS_LOCAL_HOST), PropertyLoader.getLocalPort(), 3));

        ShardedConnectionFactoryProvider connectionFactoryProvider = new ShardedConnectionFactoryProvider(specProvider);

        // Spring container would call this automatically after properties are set
        connectionFactoryProvider.afterPropertiesSet();

        RedisSerializer<String> stringSerializer = new StringRedisSerializer();

        ShardedRedisTemplateProxy<String, String> proxy = new ShardedRedisTemplateProxy<>();
        proxy.setShardingStrategy(modStrategy);
        proxy.setConnectionFactoryProvider(connectionFactoryProvider);
        proxy.setKeySerializer(stringSerializer);
        proxy.setValueSerializer(stringSerializer);

        // Spring container would call this automatically after properties are set
        proxy.afterPropertiesSet();

        // Could also get template by key. Index here for cases when we know shard upfront
        proxy.template(0).opsForValue().set("test", "0");
        proxy.template(1).opsForValue().set("test", "1");

        assertEquals("0", proxy.template(0).opsForValue().get("test"));
        assertEquals("1", proxy.template(1).opsForValue().get("test"));
    }

    @Test
    public void showcaseResilientConnFactoryUsage() throws Exception {
        RedisSentinelSpec redisSentinelSpec = RedisSentinelSpec.fromSentinelDetails(
            PropertyLoader.getProperty(PropertyKeys.REDIS_MASTER),
            PropertyLoader.getSentinel1HostPort(),
            PropertyLoader.getProperty(PropertyKeys.REDIS_PASSWORD), 1
        );

        JedisConnectionFactoryProvider connectionFactoryProvider = new JedisConnectionFactoryProvider(redisSentinelSpec);

        // Spring container would call this automatically after properties are set
        connectionFactoryProvider.afterPropertiesSet();

        RedisSerializer<String> stringSerializer = new StringRedisSerializer();

        RedisTemplateProxy<String, String> proxy = new RedisTemplateProxy<>();
        proxy.setConnectionFactoryProvider(connectionFactoryProvider);
        proxy.setKeySerializer(stringSerializer);
        proxy.setValueSerializer(stringSerializer);

        // Spring container would call this automatically after properties are set
        proxy.afterPropertiesSet();

        // Could also get template by key. Index here for cases when we know shard upfront
        proxy.getTemplate().opsForValue().set("test", "0");

        assertEquals("0", proxy.getTemplate().opsForValue().get("test"));
    }

}
