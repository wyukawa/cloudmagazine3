package cloud.magazine.vol3;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

//正規化用Reducer    
public class NormarizeReducer extends Reducer<Text, Text, NullWritable, Text> {

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		int counter = 0;// 出現したメニューの総計

		// メニューごとの出現頻度を記録するためのHashMap
		HashMap<String, IntWritable> menuCounterMap = new HashMap<String, IntWritable>();
		Iterator<String> menuIterator;

		// レコードをスキャンし、ある利用者があるメニューを食べた回数を記録
		for (Text menu : values) {
			if (menuCounterMap.containsKey(menu.toString())) {

				IntWritable numOfMenu = menuCounterMap.get(menu.toString());
				numOfMenu.set(numOfMenu.get() + 1); // 同じメニューが出現するごとにカウンターに+1
			} else {

				menuCounterMap.put(menu.toString(), new IntWritable(1)); // 新しいメニューが出現したら、専用のカウンターを作る。
			}

			counter++; // 出現したメニューの総計を+1

		}
		menuIterator = menuCounterMap.keySet().iterator();

		while (menuIterator.hasNext()) {

			String menu = menuIterator.next();

			// メニューごとに、食べた回数を総計で割って正規化する。
			// 正規化後の解釈は「食べた割合」になる。
			double rate = menuCounterMap.get(menu).get() / (double) counter;
			String userName = key.toString();

			Text outputRecord = new Text(new StringBuilder(userName)
					.append(",").append(menu).append(",").append(rate)
					.toString());

			context.write(NullWritable.get(), outputRecord);
		}
	}
}
