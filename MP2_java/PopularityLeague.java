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
        
        Job jobB = Job.getInstance(conf, "League");
        jobB.setOutputKeyClass(IntWritable.class);
        jobB.setOutputValueClass(IntWritable.class);

        jobB.setMapOutputKeyClass(IntWritable.class);
        jobB.setMapOutputValueClass(IntWritable.class);

        jobB.setMapperClass(LeagueMap.class);
        jobB.setReducerClass(LeagueReduce.class);
        jobB.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(jobB, tmpPath);
        FileOutputFormat.setOutputPath(jobB, new Path(args[1]));

        jobB.setInputFormatClass(KeyValueTextInputFormat.class);
        jobB.setOutputFormatClass(TextOutputFormat.class);

        jobB.setJarByClass(PopularityLeague.class);
        return jobB.waitForCompletion(true) ? 0 : 1;
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
        private HashSet<Integer> league = new HashSet<>();
        
        @Override
        protected void setup(
                Context context
                ) throws IOException,InterruptedException {
            Configuration conf = context.getConfiguration();
            String path = conf.get("league");
            for (String line : readHDFSFile(path, conf).split("\n")) {
                this.league.add(Integer.parseInt(line));
            }
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
                if (this.league.contains(token)) {
                    context.write(new IntWritable(token), new IntWritable(1));
                }
            }
        }
        
        @Override
        protected void cleanup(
                Context context
                ) throws IOException, InterruptedException {
            for (Integer member : this.league) {
                context.write(new IntWritable(member), new IntWritable(0));
            }
        }
    }
    
    public static class LinkCountReduce
            extends Reducer<IntWritable, IntWritable,
                            IntWritable, IntWritable> {
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
    
    public static class LeagueMap
            extends Mapper<Text, Text, NullWritable, IntArrayWritable> {
        private HashMap<Integer, Integer> map = new HashMap<>();
        private ArrayList<Integer> league = new ArrayList<>();
        
        @Override
        protected void setup(
                Context context
                ) throws IOException,InterruptedException {
            Configuration conf = context.getConfiguration();
            String path = conf.get("league");
            for (String line : readHDFSFile(path, conf).split("\n")) {
                this.league.add(Integer.parseInt(line));
            }
        }
        
        @Override
        public void map(
                Text key, Text value, Context context
                ) throws IOException, InterruptedException {
            int id = Integer.parseInt(key.toString());
            int count = Integer.parseInt(value.toString());
            this.map.put(id, count);
        }
        
        @Override
        protected void cleanup(
                Context context
                ) throws IOException, InterruptedException {
            Collections.sort(this.league, Collections.reverseOrder());
            for (Integer member : this.league) {
                int count = this.map.get(member);
                int rank = 0;
                for (Integer value : this.map.values()) {
                    rank += (count > value ? 1 : 0);
                }
                Integer[] array = {count, rank};
                IntArrayWritable writable = new IntArrayWritable(array);
                context.write(NullWritable.get(), writable);
            }
        }
    }
    
    public static class LeagueReduce
            extends Reducer<NullWritable, IntArrayWritable,
                            IntWritable, IntWritable> {
        @Override
        public void reduce(
                NullWritable key, Iterable<IntArrayWritable> values,
                Context context
                ) throws IOException, InterruptedException {
            for (IntArrayWritable value : values) {
                IntWritable[] texts = Arrays.copyOf(
                    value.get(), value.get().length, IntWritable[].class);
                context.write(texts[0], texts[1]);
            }
        }
    }
    // TODO - END MY CODE
}
