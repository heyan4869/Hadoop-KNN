import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import Utils.ARFFInputformat;
import Utils.ARFFOutputFormat;
import Utils.SparseVector;
import Utils.Vector2SF;

/**
 * KNNDriver assigns the jobs to each class and let the program process in order 
 * 
 * This is wrote by Yan He inspired by the original work that done by Song Liu (sl9885)
 */
public class KNNDriver extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new KNNDriver(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        // config a job and start it
        Configuration conf = getConf();
        conf.set("mapred.textoutputformat.separator", ",");

        // look at the test folder
        for (FileStatus fs : FileSystem.get(conf).listStatus(new Path(args[2]))) {
            conf.set("test_data", fs.getPath().toString());
            System.out.println("The current test file is "
                    + conf.get("org.niubility.learning.test"));
            
            // asign the job to each class seperately
			Job job = new Job(conf, "KNN Classifier");
            job.setJarByClass(KNNDriver.class);

            job.setMapperClass(KNNMapper.class);
            job.setReducerClass(KNNReducer.class);
            job.setCombinerClass(KNNCombiner.class);
            job.setOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Vector2SF.class);
            job.setOutputValueClass(SparseVector.class);
            job.setInputFormatClass(ARFFInputformat.class);
            job.setOutputFormatClass(ARFFOutputFormat.class);
            //job.setOutputKeyClass(Text.class);
            //job.setOutputValueClass(Text.class);
            
            // read the directory of the input file
            FileInputFormat.addInputPath(job, new Path(args[0]));
            
            // write the output in the output directory
            Path out = new Path(args[1]);
            FileSystem.get(conf).delete(out, true);
            FileOutputFormat.setOutputPath(job, out);
            
            // set the current testing file
            int res = job.waitForCompletion(true) ? 0 : 1;
            if (res != 0) {
                return res;
            }
        }
        return 0;
    }
}