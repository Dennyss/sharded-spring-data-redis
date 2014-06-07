package org.oiavorskyi.ssdr;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.util.Assert;

// TODO: Refactor to use two different providers (and pass respective parameters)
public abstract class ShardedConnectionFactoryProvider implements InitializingBean {

    private RedisShardSpecRegistry specProvider;

    private boolean resilient = false;

    private RedisConnectionFactory[] factories;

    private int shardsCount;

    // TODO: Use size of registry
    public ShardedConnectionFactoryProvider( RedisShardSpecRegistry specProvider,
                                             int shardsCount ) {
        this.specProvider = specProvider;
        this.shardsCount = shardsCount;
    }

    public boolean isResilient() {
        return resilient;
    }

    public void setResilient( boolean resilient ) {
        this.resilient = resilient;
    }

    public RedisConnectionFactory getConnectionFactory( int shardId ) {
        Assert.isTrue(shardId >= 0 && shardId < shardsCount,
                "shardId should be between 0 and " + shardsCount);
        Assert.notNull(factories, "ensure that afterPropertiesSet method was called prior to " +
                "using this method");

        return factories[shardId];
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        Assert.notNull(specProvider, "specProvider should be set prior to bean usage");
        Assert.isTrue(shardsCount > 0, "shards count should be at least 1");

        factories = new RedisConnectionFactory[shardsCount];

        for ( int i = 0; i < shardsCount; i++ ) {
            RedisShardSpec shardSpec = specProvider.getShardSpecById(i);
            factories[i] = resilient ? createResilientConnectionFactory(shardSpec) :
                    createConnectionFactory(shardSpec);
        }
    }

    protected abstract RedisConnectionFactory createConnectionFactory( RedisShardSpec shardSpec );

    protected abstract RedisConnectionFactory createResilientConnectionFactory(
            RedisShardSpec shardSpec );

}
