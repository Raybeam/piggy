package string_utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
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
import date_utils.DataChecker;

public class ExtractJsonKeys extends EvalFunc<Tuple> {

	public Tuple exec(Tuple input) throws IOException {
		List<Tuple> bagtuples = new ArrayList<Tuple>();
		Tuple result = TupleFactory.getInstance().newTuple(bagtuples);

		try {
			if (DataChecker.isValid(input, 1)) {
				String json = "{}";
				if (input.get(0) != null) {
					json = input.get(0).toString();
					json = json.replace("\\", "");
				}
				ArrayList<String> keys = new ArrayList<String>();
				JSONObject jsonObject = JSONObject.fromObject(json);
				Iterator it = jsonObject.keys();
				while (it.hasNext()) {
					String key = it.next().toString();
					keys.add(key);
				}
				Collections.sort(keys);
				for(String key : keys)
				{
					result.append(key);
				}
			}
		} catch (ExecException e) {
			e.printStackTrace();
		} catch (JSONException e) {
			return result;
		}
		return result;
	}

	public Schema outputSchema(Schema input) {
		return new Schema(new Schema.FieldSchema(null, DataType.CHARARRAY));
	}
}
