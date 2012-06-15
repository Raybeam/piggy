package test;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Before;
import org.junit.Test;

import date_utils.ConvertDateFormat;

public class TestConvertDateFormat {
	
	List<Tuple> bagtuples;
	Tuple input;
	
	@Before
	public void setUp() throws Exception {
		bagtuples = new ArrayList<Tuple>();
		input = TupleFactory.getInstance().newTuple(bagtuples);
		input.append("MM/dd/yyyy KK:mm:ss.S a");		
		input.append("yyyy-MM-dd HH:mm:ss");
		input.append("6/6/2012 04:23:39.000 PM");
	}

	@Test
	public void testExecTuple() throws IOException {
		ConvertDateFormat cdf = new ConvertDateFormat();
		String result = cdf.exec(input);
		assertEquals(result, "2012-06-06 16:23:39");
	}
}
