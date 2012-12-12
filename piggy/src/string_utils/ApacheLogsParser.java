package string_utils;

import io_utils.FileUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import net.sf.json.JSONException;
import net.sf.json.JSONObject;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import redis.clients.jedis.Jedis;

import date_utils.DataChecker;

public class ApacheLogsParser extends EvalFunc<Tuple> {

	public Tuple exec(Tuple input) throws IOException {
		List<Tuple> bagtuples = new ArrayList<Tuple>();
		Tuple result = TupleFactory.getInstance().newTuple(bagtuples);
		String[] paramColumns = {};
		String[] logColumns = {};
		try {
			paramColumns = RedisHelper.getParamColumns();
			logColumns = RedisHelper.getLogColumns();
		} catch (Exception e1) {
			e1.printStackTrace();
		}
		try {
			if (DataChecker.isValid(input, 1)) {
				String json = "{}";
				if (input.get(0) != null) {
					json = input.get(0).toString();
					json = json.replace("\\", "");
				}
				JSONObject jsonObject = JSONObject.fromObject(json);

				for (String column : logColumns) {
					if (column.equals("X-Okl-Params")) {
						Object okl_params_json = jsonObject.get(column);
						ArrayList<Object> okl_params = getOKLParams(okl_params_json, paramColumns);
						for (Object value : okl_params){
							result.append(value);
						}
					} else {
						Object data = jsonObject.get(column);
						if (data == null) {
							data = "-";
						} else {
							data = data.toString();
						}
						result.append(data);
					}
				}
				
			}
		} catch (ExecException e) {
			e.printStackTrace();
		} catch (JSONException e) {
			for (String column : paramColumns) {
				result.append("-");
			}
		}
		return result;
	}
	
	public ArrayList<Object> getOKLParams(Object okl_params_json, String[] paramColumns) throws IOException {
		ArrayList<Object> result = new ArrayList<Object>();
		try {
			if (!okl_params_json.equals("-")) {
				for (String column : paramColumns) {
					Object data = ((JSONObject) okl_params_json).get(column);
					if (data == null) {
						data = "-";
					} else {
						data = data.toString();
					}
					result.add(data);
				}
			}
			else {
				for (String column : paramColumns) {
					result.add("-");
				}
			}	
		} catch(Exception e) {
			e.printStackTrace();
		}
			return result;
	}

	public Schema outputSchema(Schema input) {
		return new Schema(new Schema.FieldSchema(null, DataType.CHARARRAY));
	}
}
