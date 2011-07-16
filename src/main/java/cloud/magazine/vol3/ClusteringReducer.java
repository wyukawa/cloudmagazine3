package cloud.magazine.vol3;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

//クラスタリング用Reducer
public class ClusteringReducer extends Reducer<Text, Text, NullWritable, Text> {

	// 類似度。平方の和の平方根がこの値以下だった利用者が好んで食べているものを推薦する
	private static final double SIMILARITY = 0.1;

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		double sumOfRate = 0; // 偏差の平方の和

		ArrayList<String> recommendationMenuList = new ArrayList<String>(); // 推薦対象者が食べたことの無いメニューのリスト

		for (Text menuAndSquareRate : values) {

			String[] splitRecord = menuAndSquareRate.toString().split(",");
			String menu = splitRecord[0];
			String squareRate = splitRecord[1];

			// 偏差の平方がNONEで渡ったら、推薦対象者が食べたことが無いものなので、知らない推薦候補のリストに格納。
			if (squareRate.equals("NONE")) {
				recommendationMenuList.add(menu);
			} else {
				sumOfRate += Double.parseDouble(squareRate);
			}
		}

		// 偏差の平方の和の平方根を計算
		sumOfRate = Math.sqrt(sumOfRate);
		Text headerMessage = new Text(new StringBuilder("--------------------")
				.append(key.toString())
				.append(" eats following foods--------------------").toString());

		// 類似度が一定値以下で、推薦候補のリストにメニューが格納されていたら推薦を実施
		if (sumOfRate <= SIMILARITY && recommendationMenuList.size() > 0) {
			context.write(NullWritable.get(), headerMessage);
			for (String recommendationMenu : recommendationMenuList) {
				context.write(NullWritable.get(), new Text(recommendationMenu));
			}
		}
	}
}
