package string_utils;

import redis.clients.jedis.Jedis;

public class RedisHelper {

	public static Jedis getRedis()
	{
		Jedis jedis = new Jedis("54.243.176.8");
		return jedis;
	}
}
