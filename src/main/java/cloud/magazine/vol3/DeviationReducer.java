package cloud.magazine.vol3;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

//偏差計算用Reducer
public class DeviationReducer extends Reducer<Text, Text, NullWritable, Text> {

	// 閾値。割合がこの値以上だったメニューを推薦する。
	private static final double THRESHOLD = 0.1;

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		Configuration conf = context.getConfiguration();
		String targetName = conf.get("targetName"); // 推薦対象者の名前を取得
		double targetRate = 0; // 推薦対象者がreduce関数に渡されたメニューを食べる頻度
		HashMap<String, Double> userAndRateMap = new HashMap<String, Double>(); // 利用者とメニューを食べる割合の対
		Iterator<String> userIterator;

		for (Text userAndRate : values) {

			// レコードから利用者の名前とメニューを食べる割合を抽出
			String[] splitRecord = userAndRate.toString().split(",");
			String userName = splitRecord[0];
			double rate = Double.parseDouble(splitRecord[1]);

			// 推薦対象者がメニューを食べる頻度は区別して記録
			if (userName.equals(targetName)) {
				targetRate = rate;
			}

			// 利用者とメニューを食べる頻度を対応付ける
			else {
				userAndRateMap.put(userName, rate);
			}
		}

		userIterator = userAndRateMap.keySet().iterator();

		// 利用者ごとに、推薦対象者との偏差を計算
		while (userIterator.hasNext()) {
			String userName = userIterator.next();
			double rate = userAndRateMap.get(userName);

			// 推薦対象者との偏差を計算
			// targetRate == 0の場合は推薦対象者がreduce関数に渡されたメニューを食べたことがないということなので
			// NONEを設定。
			String deviation = targetRate != 0 ? String.valueOf(targetRate
					- rate) : "NONE";

			Text outputRecord = new Text(new StringBuilder(userName)
					.append(",").append(key.toString()).append(",")
					.append(deviation).toString());

			// レコードを出力する。
			// NONEだったレコードは推薦対象者への推薦候補となる。
			// ただし、利用者が一定の割合以上食べていなければ
			// 推薦候補としない。
			if (!(deviation.equals("NONE") && rate < THRESHOLD)) {
				context.write(NullWritable.get(), outputRecord);
			}
		}

	}
}
