package date_utils;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import date_utils.DataChecker;

import org.apache.pig.EvalFunc;
import org.apache.pig.PigWarning;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

public class TimeZoneConverter extends EvalFunc<String> {

	/*
	 * (non-Javadoc)
	 * @see org.apache.pig.EvalFunc#exec(org.apache.pig.data.Tuple)
	 * Tuple should be (date, date_format, from_timezone, to_timezone)
	 */
	public String exec(Tuple input) throws IOException {
			
		if (DataChecker.isNull(input))
			return "";
		
		if (DataChecker.isValid(input, 5))
			return "";
		
		String date = input.get(0).toString();
		String dateFormat = input.get(1).toString();
		String fromTimeZone = input.get(2).toString();
		String toTimeZone = input.get(3).toString();
		Date d = null;

		DateFormat df1 = new SimpleDateFormat(dateFormat);
		df1.setTimeZone(TimeZone.getTimeZone(fromTimeZone));

		try {
			d = df1.parse(date);
		} catch (Exception e) {
			warn("Could not parse date: " + date + " with format: "
					+ dateFormat, PigWarning.UDF_WARNING_1);
			return "";
		}

		DateFormat df2 = new SimpleDateFormat(dateFormat);
		df2.setTimeZone(TimeZone.getTimeZone(toTimeZone));

		return df2.format(d);
	}

	public Schema outputSchema(Schema input) {
		return new Schema(new Schema.FieldSchema(null, DataType.CHARARRAY));
	}
}
