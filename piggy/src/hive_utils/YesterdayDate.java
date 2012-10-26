package hive_utils;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.Calendar;
import java.util.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

import org.apache.hadoop.io.Text;

public class YesterdayDate extends UDF {

	public Text evaluate() {
		Calendar cal = Calendar.getInstance();
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

//		System.out.println("Today's date is "
//				+ dateFormat.format(cal.getTime()));

		cal.add(Calendar.DATE, -1);
		
//		System.out.println("Yesterday's date was "
//				+ dateFormat.format(cal.getTime()));

		String date = dateFormat.format(cal.getTime()).toString();
		Text datx = new Text();
		datx.set(date);
		
		return datx;
	}
}
