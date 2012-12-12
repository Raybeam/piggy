package string_utils;

import io_utils.FileUtils;

import java.io.IOException;

import redis.clients.jedis.Jedis;

public class RedisHelper {

	public static Jedis getRedis()
	{
		Jedis jedis = new Jedis("54.243.176.8");
		return jedis;
	}
	
	public static String[] getLogColumns() throws Exception {
		String filename = "log_columns.txt";
		String rawParams = "";

		if (FileUtils.fileExists(filename)) {
			try {
				rawParams = FileUtils.readFile(filename);
			} catch (IOException e) {
				e.printStackTrace();
				throw e;
			}
		} else {
			Jedis redis = RedisHelper.getRedis();
			rawParams = redis.get("log_columns");
			try {
				FileUtils.writeToFile(rawParams, filename);
			} catch (Exception e) {
				e.printStackTrace();
				throw e;
			}
			redis.disconnect();
		}

		String[] params = rawParams.split(",");

		return params;
	}
	
	public static String[] getParamColumns() throws Exception {

		String filename = "param_columns.txt";
		String rawParams = "";

		if (FileUtils.fileExists(filename)) {
			try {
				rawParams = FileUtils.readFile(filename);
			} catch (IOException e) {
				e.printStackTrace();
				throw e;
			}
		} else {
			Jedis redis = RedisHelper.getRedis();
			rawParams = redis.get("param_columns");
			try {
				FileUtils.writeToFile(rawParams, filename);
			} catch (Exception e) {
				e.printStackTrace();
				throw e;
			}
			redis.disconnect();
		}

		String[] params = rawParams.split(",");

		return params;
	}

}
