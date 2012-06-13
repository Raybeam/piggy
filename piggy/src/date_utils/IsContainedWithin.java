package date_utils;

import java.io.IOException;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Days;

public class IsContainedWithin extends EvalFunc<Integer> {

	/**
	 * Expects a tuple like (date_format, start_date, end_date, date)
	 * Returns 1 if the date is in between start and end dates
	 * Returns 0 if start_date is null
	 * Returns 0 if less than 4 arguments are passed
	 */
	public Integer exec(Tuple input) throws IOException {
		DateTimeZone.setDefault(DateTimeZone.UTC);
		DateFormat formatter = new SimpleDateFormat(input.get(0).toString());
		//correct number of args passed
		if (!acceptableInput(input))
			return 0;
					
		//if begin date is null
		if(input.get(1).toString() == "" || input.get(1).toString() == null)
			return 0;
		
		try {
			Date startDate = (Date) formatter.parse(input.get(1).toString());
			Date endDate = getEndDate(input.get(2).toString(), formatter);
			Date date = (Date) formatter.parse(input.get(3).toString());
			if((startDate.after(date) || startDate.equals(date)) && endDate == null)
			{
				return 1;
			}
			else if((startDate.after(date) || startDate.equals(date)) && (endDate.before(date) || endDate.equals(date)))
			{
				return 1;
			}
		} catch (ParseException e) {
			e.printStackTrace();
		}
		
		return 0;
	}
	
	@Override
	public Schema outputSchema(Schema input) {
		return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass()
				.getName().toLowerCase(), input), DataType.INTEGER));
	}

	public Date getEndDate(String endDate, DateFormat formatter) throws ParseException
	{
		if(endDate != null && !endDate.equals(""))
			{
				Date enddate = (Date) formatter.parse(endDate);
				return enddate;
			}
		else
			return null;
	}
	
	public boolean acceptableInput(Tuple input)
	{
		if (input.size() == 4)
			{
				System.out.println("Wrong number of args");
				return true;
			}
		return false;
	}
	
	public static void main(String args[]) {
		try {
			String str_date = "11-June-07";
			String str_date2 = "11-June-08";
			DateFormat formatter;
			Date date;
			formatter = new SimpleDateFormat("dd-MMM-yy");
			date = (Date) formatter.parse(str_date);
			Date date2 = (Date) formatter.parse(str_date2);
			if(date2.after(date))
				System.out.println("yep");
			java.sql.Timestamp timeStampDate = new Timestamp(date.getTime());
			System.out.println("Today is " + timeStampDate);

			formatter = new SimpleDateFormat("MM/dd/yyyy KK:mm:ss.S a");
			String strd = "6/6/2012 04:04:57.000 PM";
			date = (Date) formatter.parse(strd);
			String d = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss").format(date);
			System.out.println(d);
			
		} catch (ParseException e) {
			System.out.println("Exception :" + e);
		}
	}

}
