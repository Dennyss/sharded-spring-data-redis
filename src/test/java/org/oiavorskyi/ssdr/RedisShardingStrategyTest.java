package org.oiavorskyi.ssdr;

import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Created by Denys Kovalenko on 6/18/2014.
 */
public class RedisShardingStrategyTest {
    RedisShardingStrategy redisShardingStrategy = new RedisShardingStrategyImpl(10);

    @Test(expected = IllegalArgumentException.class)
    public void constructorWrongInputTest(){
        new RedisShardingStrategyImpl(-1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void getShardIdByKeyNullInputTest(){
        redisShardingStrategy.getShardIdByKey(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void getShardIdByKeyEmptyInputTest(){
        redisShardingStrategy.getShardIdByKey(" ");
    }

    @Test
    public void shouldReturnShardNumber(){
        int shardId = redisShardingStrategy.getShardIdByKey("1C4RJFDJ2EC169760");
        assertTrue(shardId > 0);
    }

    @Test
    public void shouldReturnSameShardNumberForSameKeyManyTimes(){
        int shardId1 = redisShardingStrategy.getShardIdByKey("1C4RJFDJ2EC169760");
        int shardId2 = redisShardingStrategy.getShardIdByKey("1C4RJFDJ2EC169760");
        // This assert proofs that the function does not random behavior
        assertEquals(shardId1, shardId2);
    }

}
