package test;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Before;
import org.junit.Test;
import string_utils.UserAgentParser;

public class TestUserAgentParser {

	List<Tuple> bagtuples;
	Tuple input;
	
	@Before
	public void setUp() throws Exception {
		bagtuples = new ArrayList<Tuple>();
		String userAgent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_6_8) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1";
		
		input = TupleFactory.getInstance().newTuple(bagtuples);
		input.append(userAgent);
	}

	@Test
	public void testNull() throws IOException {
		UserAgentParser ua = new UserAgentParser();
		String agent = null;
		input = TupleFactory.getInstance().newTuple(bagtuples);
		input.append(agent);

		Tuple result = ua.exec(input);
		result = ua.exec(input);
		System.out.println(result.toString());
	}

	@Test
	public void testAndroid() throws IOException {
		UserAgentParser ua = new UserAgentParser();
		String agent = "Mozilla/5.0 (Linux; <Android Version>; <Build Tag etc.>) AppleWebKit/<WebKit Rev> (KHTML, like Gecko) Chrome/<Chrome Rev> Mobile Safari/<WebKit Rev>";
		input = TupleFactory.getInstance().newTuple(bagtuples);
		input.append(agent);

		Tuple result = ua.exec(input);
		result = ua.exec(input);
		assertEquals(result.get(3),"mobile");
		System.out.println(result.toString());
	}

	@Test
	public void testIphone() throws IOException {
		UserAgentParser ua = new UserAgentParser();
		String agent = "Mozilla/5.0 (iPhone; U; CPU like Mac OS X; en) AppleWebKit/420+ (KHTML, like Gecko) Version/3.0 Mobile/1C25 Safari/419.3";
		input = TupleFactory.getInstance().newTuple(bagtuples);
		input.append(agent);

		Tuple result = ua.exec(input);
		result = ua.exec(input);
		assertEquals(result.get(3),"mobile");
		System.out.println(result.toString());
	}

	@Test
	public void testIpad() throws IOException {
		UserAgentParser ua = new UserAgentParser();
		String agent = "Mozilla/5.0(iPad; U; CPU iPhone OS 3_2 like Mac OS X; en-us) AppleWebKit/531.21.10 (KHTML, like Gecko) Version/4.0.4 Mobile/7B314 Safari/531.21.10";
		input = TupleFactory.getInstance().newTuple(bagtuples);
		input.append(agent);

		Tuple result = ua.exec(input);
		result = ua.exec(input);
		assertEquals(result.get(3),"mobile");
		System.out.println(result.toString());
	}

	@Test
	public void testDesktop() throws IOException {
		UserAgentParser ua = new UserAgentParser();
		String agent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_4) AppleWebKit/536.25 (KHTML, like Gecko) Version/6.0 Safari/536.25";
		input = TupleFactory.getInstance().newTuple(bagtuples);
		input.append(agent);
		Tuple result = ua.exec(input);
		result = ua.exec(input);
		assertEquals(result.get(3),"desktop");
		System.out.println(result.toString());
	}

	@Test
	public void testExecTuple() throws IOException {
		UserAgentParser ua = new UserAgentParser();
		Tuple result = ua.exec(input);
		System.out.println(result);
		
		String t2 = "Mozilla/5.0 (Linux; U; Android 4.0.4; en-us; GT-P5113 Build/IMM76D) AppleWebKit/534.30 (KHTML, like Gecko) Version/4.0 Safari/534.30";
		t2 = "Mozilla/5.0 (iPhone; CPU iPhone OS 6_0 like Mac OS X) AppleWebKit/536.26 (KHTML, like Gecko) Version/6.0 Mobile/10A403 Safari/8536.25";
		input = TupleFactory.getInstance().newTuple(bagtuples);
		input.append(t2);
		result = ua.exec(input);
		System.out.println(result.toString());
	}
	
	
}
