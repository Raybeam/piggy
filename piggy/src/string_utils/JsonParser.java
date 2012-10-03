package string_utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import net.sf.json.JSONException;
import net.sf.json.JSONObject;

import org.apache.pig.EvalFunc;
import org.apache.pig.PigWarning;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import date_utils.DataChecker;

public class JsonParser extends EvalFunc<Tuple> {

	public Tuple exec(Tuple input) throws IOException {
		List<Tuple> bagtuples = new ArrayList<Tuple>();
		;
		Tuple result = TupleFactory.getInstance().newTuple(bagtuples);
		String[] paramColumns = getParamColumns();
		String jj = "";
		try {
			if (DataChecker.isValid(input, 1)) {
				String json = "{}";
				if (input.get(0) != null) {
					json = input.get(0).toString();
					json = json.replace("\\", "");
				}
				jj = json;
				JSONObject jsonObject = JSONObject.fromObject(json);
				for (String column : paramColumns) {
					String data = (String) jsonObject.get(column);
					if (data == null) {
						data = "-";
					}
					result.append(data);
				}
			}
		} catch (ExecException e) {
			e.printStackTrace();
		} 
		catch (JSONException e) {
			result.append(jj);
			for (String column : paramColumns) {
				result.append("-");
			}
		}
		return result;
	}

	public String[] getParamColumns() {
		String[] paramColumns = { "cart_id", "cart_line_id", "sales_event_id",
				"product_id", "ds_cat_id", "l1",
				"l2", "sku_id", "quantity", "payment_card_id" };

		return paramColumns;
	}

	public Schema outputSchema(Schema input) {
		return new Schema(new Schema.FieldSchema(null, DataType.CHARARRAY));
	}
}
