package date_utils;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.pig.EvalFunc;
import org.apache.pig.PigWarning;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

public class IsContainedWithin extends EvalFunc<Integer> {

	/**
	 * Expects a tuple like (date_format, start_date, end_date, date) Returns 1
	 * if the date is in between start and end dates Returns 0 if start_date is
	 * null Returns 0 if less than 4 arguments are passed
	 */
	public Integer exec(Tuple input) throws IOException {
		DateFormat formatter = new SimpleDateFormat(input.get(0).toString());
		try {
			// correct number of args passed
			if (DataChecker.isValid(input, 5))
				{	
					warn("Wrong number of args", PigWarning.UDF_WARNING_1);
					return 0;
				}

			// if begin date is null
			if (input.get(1).toString() == ""
					|| input.get(1).toString() == null)
				return 0;
			
			try {
				Date startDate = (Date) formatter
						.parse(input.get(1).toString());
				Date endDate = getEndDate(input.get(2).toString(), formatter);
				Date date = (Date) formatter.parse(input.get(3).toString());
								
				if ((date.after(startDate) || date.equals(startDate)) && endDate == null) {
					return 1;
				} else if ((date.after(startDate) || date.equals(startDate))
						&& (endDate.after(date) || endDate.equals(date))) {
					return 1;
				}
			} catch (ParseException e) {
				warn("Parse Exception" + input.get(1).toString(),
						PigWarning.UDF_WARNING_1);
				e.printStackTrace();
				return 0;
			}
		} catch (Exception e) {
			warn("Exception" + input.get(1).toString(),
					PigWarning.UDF_WARNING_1);
			return 0;
		}
		return 0;
	}

	@Override
	public Schema outputSchema(Schema input) {
		return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass()
				.getName().toLowerCase(), input), DataType.INTEGER));
	}

	public Date getEndDate(String endDate, DateFormat formatter)
			throws ParseException {
		if (endDate != null && !endDate.equals("")) {
			Date enddate = (Date) formatter.parse(endDate);
			return enddate;
		} else
			return null;
	}
}
