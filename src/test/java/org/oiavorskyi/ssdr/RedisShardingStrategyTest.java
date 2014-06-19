package org.oiavorskyi.ssdr;

import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Created by Denys Kovalenko on 6/18/2014.
 */
public class RedisShardingStrategyTest {


    @Test
    public void testShardingLogic(){
        // Incoming parameters check

        // shardsNumber value correctness test
        Exception negativeShardNumberCaseException = null;
        try {
            new RedisShardingStrategyImpl(-1);
        }catch(Exception e){
            negativeShardNumberCaseException = e;
        }
        assertTrue(negativeShardNumberCaseException instanceof IllegalArgumentException);
        assertEquals("totalShardsNumber should be positive", negativeShardNumberCaseException.getMessage());

        RedisShardingStrategy redisShardingStrategy = new RedisShardingStrategyImpl(10);

        // Null key test
        Exception nullCaseException = null;
        try {
            redisShardingStrategy.getShardIdByKey(null);
        }catch(Exception e){
            nullCaseException = e;
        }
        assertTrue(nullCaseException instanceof NullPointerException);
        assertEquals("Key should not be null", nullCaseException.getMessage());

        // Empty key test
        Exception emptyCaseException = null;
        try {
            redisShardingStrategy.getShardIdByKey(" ");
        }catch(Exception e){
            emptyCaseException = e;
        }
        assertTrue(emptyCaseException instanceof IllegalArgumentException);
        assertEquals("Key should not be empty", emptyCaseException.getMessage());

        // Make sure that result (shardId) is not null and is positive
        int shardId = redisShardingStrategy.getShardIdByKey("1C4RJFDJ2EC169760");
        assertNotNull(shardId);
        assertTrue(shardId > 0);

        // Consistency check, invoke the same method with the same parameter second time
        int shardId2 = redisShardingStrategy.getShardIdByKey("1C4RJFDJ2EC169760");
        assertSame(shardId, shardId2);
    }
}
