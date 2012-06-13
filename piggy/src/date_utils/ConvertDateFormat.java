package date_utils;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import java.text.DateFormat;
import java.text.ParseException;

public class ConvertDateFormat extends EvalFunc<String>{

	/*
	 * Arg1 = Current Format string 
	 * Arg2 = New format string
	 * Arg3 = Date string
	 * Returns newly formatted string or null
	 */
	public String exec(Tuple input) throws IOException {
		DateFormat from_formatter = new SimpleDateFormat(input.get(0).toString());
		DateFormat to_formatter = new SimpleDateFormat(input.get(1).toString());
		String strDate = input.get(2).toString();
		Date date;
		String formattedDate = null;
		try {
			date = (Date) from_formatter.parse(strDate);
			formattedDate = to_formatter.format(date);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return formattedDate;
	}
	
    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(null, DataType.CHARARRAY));
    }
}
