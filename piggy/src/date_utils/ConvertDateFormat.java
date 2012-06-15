package date_utils;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.pig.EvalFunc;
import org.apache.pig.PigWarning;
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
		return convertDate(from_formatter, to_formatter, strDate);
	}
	
	public String convertDate(DateFormat fromFormatter, DateFormat toFormatter, String strDate)
	{
		Date date = null;
		String formattedDate = null;
		try {
			date = (Date) fromFormatter.parse(strDate);
			formattedDate = toFormatter.format(date);
		} catch (ParseException e) {
			warn("ConvertDateFormat Error " + date + " with format: "
					+ fromFormatter.toString(), PigWarning.UDF_WARNING_1);
			return "";
		} catch(Exception e){
			warn("ConvertDateFormat Error " + date + " with format: "
					+ fromFormatter.toString(), PigWarning.UDF_WARNING_1);
			return "";
		}
				
		return formattedDate;
	}
    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(null, DataType.CHARARRAY));
    }
}
