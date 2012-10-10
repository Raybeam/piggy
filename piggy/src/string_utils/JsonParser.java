package string_utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

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

public class JsonParser extends EvalFunc<Tuple> {

	public Tuple exec(Tuple input) throws IOException {
		List<Tuple> bagtuples = new ArrayList<Tuple>();
		;
		Tuple result = TupleFactory.getInstance().newTuple(bagtuples);
		String[] paramColumns = getParamColumns();
		try {
			if (DataChecker.isValid(input, 1)) {
				String json = "{}";
				if (input.get(0) != null) {
					json = input.get(0).toString();
					json = json.replace("\\", "");
				}
				JSONObject jsonObject = JSONObject.fromObject(json);
				for (String column : paramColumns) {
					Object data = jsonObject.get(column);
					if (data == null) {
						data = "-";
					}
					else
					{
						data = data.toString();
					}
					result.append(data);
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


//
//	public Entry getConfigs()
//	{
//		Entry entry = null;
//		try {
//			entry = Yaml.loadType(new File("ReceiptEntry.yml"), Entry.class);
//		} catch (FileNotFoundException e) {
//			e.printStackTrace();
//		}
//		return entry;
//	}
//	
	public String[] getParamColumns() {
		
		String[] paramColumns = {"cart_id", "cart_line_id", "ds_cat_id", "l1",
				"l2", "order_id", "payment_card_id", "product_id", "quantity",
				"sales_event_id", "sku_id", "sort_by", "is_default",
				"max_depth", "exit_depth", "category_name", "vendor_id",
				"address_id", "enter_depth", "total_page_depth", "is_active" };

		Jedis redis = RedisHelper.getRedis();
		String[] params = redis.get("param_columns").split(",");

		redis.disconnect();
		
		return params;
	}

	public Schema outputSchema(Schema input) {
		return new Schema(new Schema.FieldSchema(null, DataType.CHARARRAY));
	}
}
