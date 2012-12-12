package test;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Before;
import org.junit.Test;

import redis.clients.jedis.Jedis;
import string_utils.ApacheLogsParser;
import string_utils.RedisHelper;
import string_utils.UserAgentParser;

public class TestApacheLogsParser {

	List<Tuple> bagtuples;
	Tuple input1, input2, input3;

	@Before
	public void setUp() throws Exception {
		bagtuples = new ArrayList<Tuple>();
		String json3 = "{\"X-Forwarded-For\":\"10.110.3.22\",\"Remote-Logname\":\"-\",\"Remote-User\":\"-\",\"Timestamp\":\"10/Dec/2012:13:00:00 -0800\",\"X-Okl-Session-Id\":\"q3frs4icjll1vi944bovgishq6\",\"X-Okl-Customer-Id\":\"-\",\"Processing-Microsecs\":\"-\",\"Response-Microsecs\":\"76022\",\"Method\":\"GET\",\"Path\":\"/v1/directed_shopping_category/from_slug.json?slug_arr%5B%5D=furniture-lighting\",\"Scheme\":\"HTTP/1.1\\\",\"Status\":\"200\",\"Payload-Size\":\"126\",\"Referer\":\"-\",\"User-Agent\":\"Ruby\",\"Server-Name\":\"int01.newokl.com\",\"X-OKL-Visitor-Id\":\"f34cf43e-32eb-41c1-9158-f4fe9eb18dca\",\"X-Okl-Action-Name\":\"-\",\"X-Okl-Request-Type\":\"-\",\"X-Okl-Source\":\"-\",\"X-Okl-Utm-Campaign\":\"-\",\"X-Okl-Utm-Term\":\"-\",\"X-Okl-Utm-Medium\":\"-\",\"X-Okl-Utm-Content\":\"-\",\"X-Okl-Params\":\"-\",\"X-Okl-Action-Type\":\"-\",\"X-Okl-Experiment\":\"-\",\"X-Okl-Rails-Session-Id\":\"-\",\"X-Okl-Ref-Link\":\"-\",\"X-Okl-Ref-Action-Name\":\"-\"}";		
		String json2 = "{\"X-Forwarded-For\":\"10.110.3.22\",\"Remote-Logname\":\"-\",\"Remote-User\":\"-\",\"Timestamp\":\"12/Dec/2012:08:00:00 -0800\",\"X-Okl-Session-Id\":\"6mjtc4cl5v8i4nkdmfgi2n9sv3\",\"X-Okl-Customer-Id\":\"-\",\"Processing-Microsecs\":\"-\",\"Response-Microsecs\":\"77860\",\"Method\":\"GET\",\"Path\":\"/v1/directed_shopping_category/from_slug.json?slug_arr%5B%5D=furniture-lighting\",\"Scheme\":\"HTTP/1.1\\\",\"Status\":\"200\",\"Payload-Size\":\"126\",\"Referer\":\"-\",\"User-Agent\":\"Ruby\",\"Server-Name\":\"int01.newokl.com\",\"X-OKL-Visitor-Id\":\"72fe28ca-61fe-4086-b708-a0c6a180baf6\",\"X-Okl-Action-Name\":\"-\",\"X-Okl-Request-Type\":\"-\",\"X-Okl-Source\":\"-\",\"X-Okl-Utm-Campaign\":\"-\",\"X-Okl-Utm-Term\":\"-\",\"X-Okl-Utm-Medium\":\"-\",\"X-Okl-Utm-Content\":\"-\",\"X-Okl-Params\":\"-\",\"X-Okl-Action-Type\":\"-\",\"X-Okl-Experiment\":\"-\",\"X-Okl-Rails-Session-Id\":\"-\",\"X-Okl-Ref-Link\":\"-\",\"X-Okl-Ref-Action-Name\":\"-\"}";
		String json1 = "{\"X-Forwarded-For\":\"10.110.3.22\",\"Remote-Logname\":\"-\",\"Remote-User\":\"-\",\"Timestamp\":\"12/Dec/2012:08:00:00 -0800\",\"X-Okl-Session-Id\":\"6mjtc4cl5v8i4nkdmfgi2n9sv3\",\"X-Okl-Customer-Id\":\"-\",\"Processing-Microsecs\":\"-\",\"Response-Microsecs\":\"77860\",\"Method\":\"GET\",\"Path\":\"/v1/directed_shopping_category/from_slug.json?slug_arr%5B%5D=furniture-lighting\",\"Scheme\":\"HTTP/1.1\\\",\"Status\":\"200\",\"Payload-Size\":\"126\",\"Referer\":\"-\",\"User-Agent\":\"Ruby\",\"Server-Name\":\"int01.newokl.com\",\"X-OKL-Visitor-Id\":\"72fe28ca-61fe-4086-b708-a0c6a180baf6\",\"X-Okl-Action-Name\":\"-\",\"X-Okl-Request-Type\":\"-\",\"X-Okl-Source\":\"-\",\"X-Okl-Utm-Campaign\":\"-\",\"X-Okl-Utm-Term\":\"-\",\"X-Okl-Utm-Medium\":\"-\",\"X-Okl-Utm-Content\":\"-\",\"X-Okl-Params\":{\"max_depth\":\"10029\",\"exit_depth\":\"10029\",\"total_page_depth\":\"10648\",\"enter_depth\":\"0\",\"sales_event_id\":\"17473\"},\"X-Okl-Action-Type\":\"-\",\"X-Okl-Experiment\":\"-\",\"X-Okl-Rails-Session-Id\":\"-\",\"X-Okl-Ref-Link\":\"-\",\"X-Okl-Ref-Action-Name\":\"-\"}";
		input1 = TupleFactory.getInstance().newTuple(bagtuples);
		input2 = TupleFactory.getInstance().newTuple(bagtuples);
		input3 = TupleFactory.getInstance().newTuple(bagtuples);
		input1.append(json1);
		input2.append(json2);
		input3.append(json3);
	}

	@Test
	public void testExecTuple() throws IOException {
		ApacheLogsParser alp = new ApacheLogsParser();
		Tuple result = alp.exec(input1);
		System.out.println("===== Test with OKL-Params <> NULL =====");
		System.out.println("result :");
		System.out.println(result);
		result = alp.exec(input2);
		System.out.println("===== Test with OKL-Params == NULL =====");
		System.out.println("result :");
		System.out.println(result);
		result = alp.exec(input3);
		System.out.println("===== Test with random selected input =====");
		System.out.println("result :");
		System.out.println(result);
	}

}
