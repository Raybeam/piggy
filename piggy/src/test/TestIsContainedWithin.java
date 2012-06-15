package test;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Before;
import org.junit.Test;

import date_utils.*;

public class TestIsContainedWithin {

	List<Tuple> bagtuples;
	Tuple input;
	
	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
		bagtuples = new ArrayList<Tuple>();
		input = TupleFactory.getInstance().newTuple(bagtuples);
		input.append("yyyy-MM-dd HH:mm:ss");
		input.append("2012-05-13 01:02:54");
		input.append("2012-06-02 01:10:12");
		input.append("2012-06-01 01:10:12");
	}
	
	@Test
	public void testExecTuple() throws IOException {
		IsContainedWithin icw = new IsContainedWithin();
		int result = icw.exec(input);
		assertEquals(result, 1);
		input = TupleFactory.getInstance().newTuple(bagtuples);
		input.append("yyyy-MM-dd HH:mm:ss");
		input.append("2012-05-13 01:02:54");
		input.append("");
		input.append("2012-06-01 01:10:12");
		result = icw.exec(input);
		assertEquals(result, 1);
	}

	@Test
	public void testGetEndDate() {
	}

	@Test
	public void testAcceptableInput() {
	}

}
