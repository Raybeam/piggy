package string_utils;

import io_utils.FileUtils;

import java.io.IOException;
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
				if(user_agent.toString().contains("oklapp"))
				{
					result.append("okl_iphone_app");
					result.append("MAC_OS_X");
					result.append("iphone");
					result.append("mobile_app");
				}
				else
				{
					result.append(ua.getBrowser().toString());
					result.append(ua.getOperatingSystem().toString());		
					ArrayList<String> deviceType = getDevice(ua
							.getOperatingSystem().toString());
					result.append(deviceType.get(0).toString());
					result.append(deviceType.get(1).toString());
				}			
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
		LinkedHashMap<String, String> osMap = new LinkedHashMap<String, String>();
		String filename = "os_maps.txt";

		String osMapJson = "";
		
		if(FileUtils.fileExists(filename))
		{
			try {
				osMapJson = FileUtils.readFile(filename);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		else
		{
			Jedis redis = RedisHelper.getRedis();
			osMapJson = redis.get("os_map");
			try {
				FileUtils.writeToFile(osMapJson,filename);
			} catch (Exception e) {
				e.printStackTrace();
			}
			redis.disconnect();
		}
		
		osMap = buildOsMap(osMapJson);
		
		return osMap;
	}

	private LinkedHashMap<String, String> buildOsMap(String osMapJson)
	{
		LinkedHashMap<String, String> osMap = new LinkedHashMap<String, String>();
		JSONObject jsonObject = JSONObject.fromObject(osMapJson);
		@SuppressWarnings("unchecked")
		Iterator<String> keys = jsonObject.keys();
		while(keys.hasNext())
		{
			String key	= keys.next().toString();
			osMap.put(key, jsonObject.getString(key).toString());
		}

		return osMap;
	}

	public Schema outputSchema(Schema input) {
		return new Schema(new Schema.FieldSchema(null, DataType.TUPLE));
	}
}
