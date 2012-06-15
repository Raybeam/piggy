package date_utils;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;

public class DataChecker {

	public static boolean isNull(Tuple input)
	{
		try {
			if(input.get(0) == null || input == null || input.size() == 0)
				return true;
		} catch (ExecException e) {
			e.printStackTrace();
		}
		return true;
	}
	
	public static boolean isValid(Tuple input, int size)
	{
		if(input.size() > size)
			return true;
		return false;
	}
}
