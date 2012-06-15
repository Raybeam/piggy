package string_utils;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

public class StringUtils  extends EvalFunc<String> {
	
	  public String exec(Tuple input) {
		 return "hello bro"; 	
	    }

	    public Schema outputSchema(Schema input) {
	        return new Schema(new Schema.FieldSchema(null, DataType.CHARARRAY));
	    }
}
