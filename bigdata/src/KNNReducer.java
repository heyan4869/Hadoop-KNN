import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import Utils.SparseVector;
import Utils.Vector2SF;

/**
 * KNNReducer compare the result of distance calculation and generate the final prediction
 * of label for each test data point and wrote the result in output file
 * 
 * This is wrote by Yan He inspired by the original work that done by Song Liu (sl9885)
 */
public class KNNReducer extends Reducer<Text, Vector2SF, Text, Text> {

	//private HashMap<Text, Text> pred = new HashMap<Text, Text>();
	
    protected void reduce(
            Text key,
            java.lang.Iterable<Vector2SF> value,
            org.apache.hadoop.mapreduce.Reducer<Text, Vector2SF, Text, Text>.Context context)
            throws java.io.IOException, InterruptedException {
    	
        ArrayList<Vector2SF> vs = new ArrayList<Vector2SF>();
        
        // sort each vector2SF by similarty
        for (Vector2SF v : value) {
            vs.add(new Vector2SF(v.getV1(), v.getV2(), v.getV3()));
            // check the content of v
       		//System.out.println("----- V1 is " + v.getV1() + " ----- V2 is " + v.getV2());
        }
        
        Collections.sort(vs, new Comparator<Vector2SF>() {

            @Override
            // compare o1 and o2
            public int compare(Vector2SF o1, Vector2SF o2) {
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
        
        
        // SparseVector sp = new SparseVector();
        // for (int i = 0; i < k && i < vs.size(); i++) {
        //     sp.put(vs.get(i).getV1(), vs.get(i).getV2());
        // }
        // context.write(key, sp);
        
        
        // decide the predict label and write into the output file
        HashMap<String, Integer> mapKey = new HashMap<String, Integer>();
        HashMap<String, Float> mapVal = new HashMap<String, Float>();
        for (int i = 0; i < k && i < vs.size(); i++) {
        	if (mapKey.containsKey(vs.get(i).getV1())) {
        		mapKey.put(vs.get(i).getV1(), mapKey.get(vs.get(i).getV1())+1);
        		mapVal.put(vs.get(i).getV1(), mapVal.get(vs.get(i).getV1())+vs.get(i).getV2());
        	} else {
        		mapKey.put(vs.get(i).getV1(), 1);
        		mapVal.put(vs.get(i).getV1(), vs.get(i).getV2());
        	}
        }
        int maxKey = 0;
        int currKey = 0;
        String maxVal = "";
        for (String str : mapKey.keySet()) {
        	currKey = mapKey.get(str);
        	if (currKey > maxKey) {
        		maxKey = currKey;
        		maxVal = str;
        	} else if (currKey == maxKey) {
        		if (mapVal.get(str) > mapVal.get(maxKey)) {
        			maxVal = str;
        		}
        	} else {
        		continue;
        	}
        }
        
        Text maxLabel = new Text(maxVal);
        //System.out.println("**** " + maxLabel);
        
        // check the predicted label
        System.out.println("The prediction is " + key + " " + maxLabel);
        
        //System.out.println("loop once");
        
        //System.out.println(key);
        
//        System.out.println("put");
//        pred.put(key, maxLabel);
//        System.out.println("done");
        
//        if (pred.size() == 30) {
//        	System.out.println("********************");
//        	System.out.println(pred.toString());
//        	System.out.println("********************");
//        }
        
          // test the key
//        Text temp = new Text();
//    	  temp.set(1+"");
    	
    	
        
        
//        if (pred.size() == 30) {
//        	
//        	System.out.println("----- The size of predict label is " + pred.size());
//        	for (int i = 0; i < 30; i++) {
//        		for (Text t : pred.keySet()) {
//        			//System.out.println(t);
//        			Text temp = new Text();
//        			temp.set(i + "");
//        			//System.out.println(temp);
//        			if (t.equals(temp)) {
//        				System.out.println("Writing the result...");
//        				context.write(t, pred.get(t));
//        				
//        			} else {
//        				continue;
//        			}
//        				
//        		}
//        	}
//        }
        
        
        context.write(key, maxLabel);
        
    }

//	private void writeToOutput(Text key, Text maxLabel) {
//		// TODO Auto-generated method stub
//		pred.put(key, maxLabel);
//		System.out.println(key + " " + maxLabel);
//		//System.out.println(" ----- The size of predict label is " + pred.size());
//		//return pred;
//		
//	}

    ;
}