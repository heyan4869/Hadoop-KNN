import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import Utils.Vector2SF;

/**
 * KNNCombiner sort the vector vs by compare the V2 value of o1 and o2
 * 
 * This is wrote by Yan He inspired by the original work that done by Song Liu (sl9885)
 */
public class KNNCombiner extends Reducer<Text, Vector2SF, Text, Vector2SF> {
	protected void reduce(
			Text key,
			java.lang.Iterable<Vector2SF> value,
			org.apache.hadoop.mapreduce.Reducer<Text, Vector2SF, Text, Vector2SF>.Context context)
			throws java.io.IOException, InterruptedException {
		ArrayList<Vector2SF> vs = new ArrayList<Vector2SF>();
		
		
		// sort each vector2SF by similarty
		for (Vector2SF v : value) {
			vs.add(new Vector2SF(v.getV1(), v.getV2(), v.getV3()));
			// check the size of vs
			//System.out.println("----- size of vs is " + vs.size());
			// check the content of v
			//System.out.println("----- V1 is " + v.getV1() + " ----- V2 is " + v.getV2());
		}
		
		// sort vs
		Collections.sort(vs, new Comparator<Vector2SF>() {
			@Override
			public int compare(Vector2SF o1, Vector2SF o2) {
				// check the content of o1 and o2				
				//System.out.println("----- V2 in o1 is " + o1.getV2() + "----- V2 in o2 is " + o2.getV2());
				
				// compare the V2 of o1 and o2
				if (o1.getV2() == o2.getV2())
				    return 0;
				else if (o1.getV2() < o2.getV2())
				    return -1;
				else
					return 1;
			    
			}
		});
		
		// find the 5 nearest points to each test data
		int k = context.getConfiguration().getInt("org.niubility.knn.k", 5);

		for (int i = 0; i < k && i < vs.size(); i++) {
			
			System.out.println("The key in combiner is " + vs.get(i).getV1());
			context.write(key, vs.get(i));
		}
	};
}