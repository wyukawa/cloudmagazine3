package cloud.magazine.vol3;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//正規化用Mapper
public class NormarizeMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	public void map(LongWritable lineOffset, Text record, Context context)
			throws IOException, InterruptedException {

		String[] splitRecord = record.toString().split(",");
		String userName = splitRecord[1];
		String menu = splitRecord[2];

		context.write(new Text(userName), new Text(menu));
	}

}
