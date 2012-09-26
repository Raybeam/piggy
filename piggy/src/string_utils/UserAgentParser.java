package string_utils;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import nl.bitwalker.useragentutils.UserAgent;

public class UserAgentParser {
		
	public String exec(Tuple input) {
		String j = new String();
		try {
			UserAgent ua = new UserAgent(input.get(0).toString());
			System.out.println(input.get(0).toString());
			j += (ua.getBrowser().toString()) + "--";
			System.out.println(ua.getBrowser().toString());
			j += (ua.getOperatingSystem().toString()) + "--";
			System.out.println(ua.getOperatingSystem().toString());
		} catch (ExecException e) {
			e.printStackTrace();
		}
		
		System.out.println("Result: " + j);
		return j;
	}

	public Schema outputSchema(Schema input) {
		return new Schema(new Schema.FieldSchema(null, DataType.CHARARRAY));
	}
}
