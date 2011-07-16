package cloud.magazine.vol3;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Test;


public class CafeteriaRecommendationTest {

	@Test
	public void testRun() throws Exception {
		JobConf conf = new JobConf();

		Path input = new Path(System.getProperty("user.dir")
				+ "/src/test/resources/cloud/magazine/vol3/cafeteria.history.201007");


		CafeteriaRecommendation cr = new CafeteriaRecommendation();
		cr.setConf(conf);

		int exitCode = cr.run(new String[] { "A", input.toString() });
		assertThat(exitCode, is(0));
	}

}
