package date_utils;


import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

public class TimeZoneUtils extends EvalFunc<String>{

	public String exec(Tuple input) throws IOException {
		String date = input.get(0).toString();
		String dateFormat = input.get(1).toString();
		String fromTimeZone = input.get(2).toString();
		String toTimeZone = input.get(3).toString();
		Date d = null;

		DateFormat df1 = new SimpleDateFormat(dateFormat);  
		df1.setTimeZone(TimeZone.getTimeZone(fromTimeZone));  
		
		try {
			d = df1.parse(date);
		} catch (ParseException e) {
			e.printStackTrace();
		}  
		
		DateFormat df2 = new SimpleDateFormat(dateFormat);  
		df2.setTimeZone(TimeZone.getTimeZone(toTimeZone));  
		
		return df2.format(d);
	}
	
    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(null, DataType.CHARARRAY));
    }
}
