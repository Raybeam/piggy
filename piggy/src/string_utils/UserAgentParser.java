package string_utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import net.sf.json.JSONObject;
import nl.bitwalker.useragentutils.UserAgent;
import org.apache.pig.EvalFunc;

import redis.clients.jedis.Jedis;

import date_utils.DataChecker;

public class UserAgentParser extends EvalFunc<Tuple> {

	public Tuple exec(Tuple input) {
		List<Tuple> bagtuples = new ArrayList<Tuple>();
		Tuple result = TupleFactory.getInstance().newTuple(bagtuples);
		try {
			if (DataChecker.isValid(input, 1)) {
				String user_agent = "";
				if (input.get(0) != null) {
					user_agent = input.get(0).toString();
				}
				UserAgent ua = new UserAgent(user_agent);
				result.append(ua.getBrowser().toString());
				result.append(ua.getOperatingSystem().toString());

				ArrayList<String> deviceType = getDevice(ua
						.getOperatingSystem().toString());
				result.append(deviceType.get(0).toString());
				result.append(deviceType.get(1).toString());
			}
		} catch (ExecException e) {
			e.printStackTrace();
		}

		return result;
	}

	private ArrayList<String> getDevice(String osName) {
		osName = osName.toLowerCase();
		ArrayList<String> result = new ArrayList<String>();
		LinkedHashMap<String, String> deviceMap = getOsTypes();

		for (String key : deviceMap.keySet()) {
			//found device name
			if (osName.contains(key)) {
				result.add(key);
				result.add(deviceMap.get(key));
				System.out.println(key + "--" + deviceMap.get(key));
				return result;
			}
		}
		// Not found in the map
		result.add("unknown_os");
		result.add("unknown_device");
		return result;
	}

	private LinkedHashMap<String, String> getOsTypes() {
		LinkedHashMap<String, String> osNames = new LinkedHashMap<String, String>();
		Jedis redis = RedisHelper.getRedis();
		String osMapJson = redis.get("os_map");
		JSONObject jsonObject = JSONObject.fromObject(osMapJson);
		@SuppressWarnings("unchecked")
		Iterator<String> keys = jsonObject.keys();
		while(keys.hasNext())
		{
			String key	= keys.next().toString();
			osNames.put(key, jsonObject.getString(key).toString());
		}
		redis.disconnect();
		return osNames;
	}

	public Schema outputSchema(Schema input) {
		return new Schema(new Schema.FieldSchema(null, DataType.TUPLE));
	}
}
