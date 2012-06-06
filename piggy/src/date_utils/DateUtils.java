package date_utils;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Days;

public class DateUtils extends EvalFunc<Integer> {

	public Integer exec(Tuple input) throws IOException {
		DateTimeZone.setDefault(DateTimeZone.UTC);

		DateTime startDate = new DateTime(input.get(1).toString());
		DateTime endDate = new DateTime(input.get(2).toString());
		Days d = Days.daysBetween(endDate, startDate);
		int days = d.getDays();
		if(days >= Integer.parseInt(input.get(0).toString()) && 
				days <= Integer.parseInt(input.get(0).toString()))
		{
			return 1;
		}
		return 0;
	}
	
	@Override
	public Schema outputSchema(Schema input) {
		return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass()
				.getName().toLowerCase(), input), DataType.INTEGER));
	}

}
