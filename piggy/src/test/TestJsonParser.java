package test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Before;
import org.junit.Test;

import string_utils.JsonParser;
import string_utils.UserAgentParser;


public class TestJsonParser {

	List<Tuple> bagtuples;
	Tuple input;
	
	@Before
	public void setUp() throws Exception {
		bagtuples = new ArrayList<Tuple>();
		String json = "{\"l2\":\"flatware-dinnerware\"}";
		input = TupleFactory.getInstance().newTuple(bagtuples);
		input.append(json);
	}

	@Test
	public void emptyTest() throws IOException {
		JsonParser jp = new JsonParser();
		String json = "";
		input = TupleFactory.getInstance().newTuple(bagtuples);
		input.append(json);
		Tuple result = jp.exec(input);
		System.out.println(result);		
	}
	
	@Test
	public void nullTest() throws IOException {
		JsonParser jp = new JsonParser();
		String json = null;
		input = TupleFactory.getInstance().newTuple(bagtuples);
		input.append(json);
		Tuple result = jp.exec(input);
		System.out.println(result);		
	}
	
	
	@Test
	public void testExecTuple() throws IOException {
		JsonParser jp = new JsonParser();
		Tuple result = jp.exec(input);
		System.out.println(result);		
	}
	
}
