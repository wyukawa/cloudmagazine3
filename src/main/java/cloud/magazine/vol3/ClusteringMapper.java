package cloud.magazine.vol3;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//クラスタリング用Mapper
public class ClusteringMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	public void map(LongWritable lineOffset, Text record, Context context)
			throws IOException, InterruptedException {

		String[] splitRecord = record.toString().split(",");
		String userName = splitRecord[0];
		String menu = splitRecord[1];

		// 偏差の平方を計算
		String squareRate = !splitRecord[2].equals("NONE") ? String
				.valueOf(Math.pow(Double.parseDouble(splitRecord[2]), 2))
				: "NONE";

		Text outputUserName = new Text(userName);
		Text menuAndSquareRate = new Text(new StringBuilder(menu).append(",")
				.append(squareRate).toString());

		context.write(outputUserName, menuAndSquareRate);
	}
}
