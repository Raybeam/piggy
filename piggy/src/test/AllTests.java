package test;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({ TestConvertDateFormat.class, TestIsContainedWithin.class,
		TestTimeZoneConverter.class, TestUserAgentParser.class, TestJsonParser.class })
public class AllTests {

}
