package hive_test;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.junit.Test;

import hive_utils.YesterdayDate;

public class TestYesterdayDate {

	@Test
	public void testYesterdayDate() throws IOException {
		YesterdayDate yd = new YesterdayDate();
		Text date = yd.evaluate();
		System.out.println(date.toString());
	}
}
