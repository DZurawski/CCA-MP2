import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

public class PopularityLeague extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new PopularityLeague(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        FileSystem fs = FileSystem.get(conf);
        Path tmpPath = new Path("./tmp");
        fs.delete(tmpPath, true);

        Job jobA = Job.getInstance(conf, "Link Count");
        jobA.setOutputKeyClass(IntWritable.class);
        jobA.setOutputValueClass(IntWritable.class);

        jobA.setMapperClass(LinkCountMap.class);
        jobA.setReducerClass(LinkCountReduce.class);

        FileInputFormat.setInputPaths(jobA, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobA, tmpPath);

        jobA.setJarByClass(PopularityLeague.class);
        jobA.waitForCompletion(true);
    }

    public static class IntArrayWritable extends ArrayWritable {
        public IntArrayWritable() {
            super(IntWritable.class);
        }

        public IntArrayWritable(Integer[] numbers) {
            super(IntWritable.class);
            IntWritable[] ints = new IntWritable[numbers.length];
            for (int i = 0; i < numbers.length; i++) {
                ints[i] = new IntWritable(numbers[i]);
            }
            set(ints);
        }
    }

    public static String readHDFSFile(String path, Configuration conf) throws IOException{
        Path pt=new Path(path);
        FileSystem fs = FileSystem.get(pt.toUri(), conf);
        FSDataInputStream file = fs.open(pt);
        BufferedReader buffIn=new BufferedReader(new InputStreamReader(file));

        StringBuilder everything = new StringBuilder();
        String line;
        while( (line = buffIn.readLine()) != null) {
            everything.append(line);
            everything.append("\n");
        }
        return everything.toString();
    }

    // TODO - MY CODE
    public static class LinkCountMap
            extends Mapper<Object, Text, IntWritable, IntWritable> {
        private HashSet<Integer> league;
        
        @Override
        protected void setup(
                Context context
                ) throws IOException,InterruptedException {
            Configuration conf = context.getConfiguration();
            String path = conf.get("league");
            String[] lines = readHDFSFile(path, conf).split("\n");
            List<Integer> list
                = Arrays.stream(lines).mapToInt(Integer::parseInt).toList();
            this.league = new HashSet<Integer>(list);
        }
        
        @Override
        public void map(
                Object key, Text value, Context context
                ) throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line, ",: ");
            int token = Integer.parseInt(tokenizer.nextToken().trim());
            while (tokenizer.hasMoreTokens()) {
                token = Integer.parseInt(tokenizer.nextToken().trim());
                if (this.league.contains(token) {
                    context.write(new IntWritable(token), new IntWritable(1));
                }
            }
        }
    }
    
    public static class LinkCountReduce
            extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        @Override
        public void reduce(
                IntWritable key, Iterable<IntWritable> values, Context context
                ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }
    
    public static class TopLinksMap extends Mapper<Text, Text, NullWritable, IntArrayWritable> {
        private List<Integer> league;
        
        @Override
        protected void setup(Context context) throws IOException,InterruptedException {
            Configuration conf = context.getConfiguration();
            String path = conf.get("league");
            String[] lines = readHDFSFile(path, conf).split("\n");
            this.league
                = Arrays.stream(lines).mapToInt(Integer::parseInt).toList();
        }

        // TODO - MY CODE
        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            Integer word = Integer.parseInt(key.toString());
            Integer count = Integer.parseInt(value.toString());
            this.set.add(new Pair<Integer, Integer>(count, word));
            if (this.set.size() > this.total) {
                this.set.remove(this.set.first());
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            while ( ! this.set.isEmpty()) {
                Pair<Integer, Integer> pair = this.set.last();
                Integer[] numbers = {pair.second, pair.first};
                this.set.remove(pair);
                IntArrayWritable array = new IntArrayWritable(numbers);
                context.write(NullWritable.get(), array);
            }
        }
    }
    // TODO - END MY CODE
}
