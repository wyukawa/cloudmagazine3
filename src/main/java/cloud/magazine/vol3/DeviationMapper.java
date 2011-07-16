package cloud.magazine.vol3;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//偏差計算用Mapper
public class DeviationMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	public void map(LongWritable lineOffset, Text record, Context context)
			throws IOException, InterruptedException {

		String[] splitRecord = record.toString().split(",");
		String userName = splitRecord[0];
		String menu = splitRecord[1];
		double rate = Double.parseDouble(splitRecord[2]);
		Text userNameAndRate = new Text(new StringBuilder(userName).append(",")
				.append(rate).toString());

		context.write(new Text(menu), userNameAndRate);

	}

}
