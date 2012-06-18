package date_utils;

import org.apache.pig.EvalFunc;
import org.apache.pig.PigWarning;
import org.apache.pig.data.Tuple;

public class DataChecker {

	public static boolean isNull(Tuple input)
	{
		try {
			if(input.get(0) == null || input == null || input.size() == 0)
				return true;
		} catch (Exception e) {
			return true;
		}
		return false;
	}
	
	public static boolean isValid(Tuple input, int size)
	{
		if(input.size() >= size)
			return true;
		return false;
	}
}
