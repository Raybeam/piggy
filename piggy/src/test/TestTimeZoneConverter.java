/**
 * 
 */
package test;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import date_utils.TimeZoneConverter;

/**
 * @author sasan
 *
 */
public class TestTimeZoneConverter {

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
	}

	/**
	 * Test method for {@link date_utils.TimeZoneConverter#exec(org.apache.pig.data.Tuple)}.
	 * @throws IOException 
	 */
	@Test
	public void testExecTuple() throws IOException {
		TimeZoneConverter tzc  = new TimeZoneConverter();
		List<Tuple> bagtuples = new ArrayList<Tuple>();
		Tuple input = TupleFactory.getInstance().newTuple(bagtuples);
		input.append("test");
		String result = tzc.exec(input);
		assertEquals(result, "");
	}	

}
