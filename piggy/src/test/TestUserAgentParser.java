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
	public void testExecTuple() throws IOException {
		UserAgentParser ua = new UserAgentParser();
		String result = ua.exec(input);
		System.out.println(result);
		
		String t2 = "Mozilla/5.0 (Linux; U; Android 4.0.4; en-us; GT-P5113 Build/IMM76D) AppleWebKit/534.30 (KHTML, like Gecko) Version/4.0 Safari/534.30";
		t2 = "Mozilla/5.0 (iPhone; CPU iPhone OS 6_0 like Mac OS X) AppleWebKit/536.26 (KHTML, like Gecko) Version/6.0 Mobile/10A403 Safari/8536.25";
		input = TupleFactory.getInstance().newTuple(bagtuples);
		input.append(t2);
		result = ua.exec(input);
		System.out.println(result);
	}
	
	
}
