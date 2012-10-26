package test;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Before;
import org.junit.Test;

import string_utils.ExtractJsonKeys;

public class TestExtractJsonKeys {
	List<Tuple> bagtuples;
	Tuple input;

	@Before
	public void setUp() throws Exception {
		bagtuples = new ArrayList<Tuple>();
		String json = "{\"sales_event_id\":\"13840\",\"product_id\":\"678417\"}";
		input = TupleFactory.getInstance().newTuple(bagtuples);
		input.append(json);
	}

	@Test
	public void emptyTest() throws IOException {
		ExtractJsonKeys jp = new ExtractJsonKeys();
		String json = "";
		input = TupleFactory.getInstance().newTuple(bagtuples);
		input.append(json);
		Tuple result = jp.exec(input);
		System.out.println(result);
		assertTrue(result.size() == 0);
	}

	@Test
	public void nullTest() throws IOException {
		ExtractJsonKeys jp = new ExtractJsonKeys();
		String json = null;
		input = TupleFactory.getInstance().newTuple(bagtuples);
		input.append(json);
		Tuple result = jp.exec(input);
		System.out.println(result);
		assertTrue(result.size() == 0);
	}

	@Test
	public void testExecTuple() throws IOException {
		ExtractJsonKeys jp = new ExtractJsonKeys();
		Tuple result = jp.exec(input);
		System.out.println(result);
		assertTrue(result.size() == 2);
	}

}
