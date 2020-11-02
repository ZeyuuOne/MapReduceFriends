import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeSet;

public class BasicFriends {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        GenericOptionsParser optionsParser = new GenericOptionsParser(conf, args);
        String[] remainingArgs = optionsParser.getRemainingArgs();

        Path tempPath = new Path("temp");

        Job job1 = Job.getInstance(conf, "transposition & pairing");
        job1.setJarByClass(BasicFriends.class);
        job1.setMapperClass(TranspositionMapper.class);
        job1.setMapOutputKeyClass(IntWritable.class);
        job1.setMapOutputKeyClass(IntWritable.class);
        job1.setNumReduceTasks(1);
        job1.setReducerClass(PairingReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job1, new Path(remainingArgs[0]));
        FileOutputFormat.setOutputPath(job1, tempPath);

        if (!job1.waitForCompletion(true)) System.exit(1);

        Job job2 = Job.getInstance(conf, "merging");
        job2.setJarByClass(BasicFriends.class);
        job2.setInputFormatClass(SequenceFileInputFormat.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(IntWritable.class);
        job2.setNumReduceTasks(1);
        job2.setReducerClass(MergingReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        job2.setOutputFormatClass(FriendsOutputFormat.class);

        FileInputFormat.addInputPath(job2, tempPath);
        FileOutputFormat.setOutputPath(job2, new Path(remainingArgs[1]));

        if (!job2.waitForCompletion(true)) System.exit(1);

        System.exit(0);

    }

    public static class TranspositionMapper extends Mapper<Object, Text, IntWritable, IntWritable> {

        IntWritable host = new IntWritable();
        IntWritable friend = new IntWritable();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().replace(",", "");
            StringTokenizer itr = new StringTokenizer(line);
            if (!itr.hasMoreTokens()) return;
            host.set(Integer.parseInt(itr.nextToken()));
            while (itr.hasMoreTokens()) {
                friend.set(Integer.parseInt(itr.nextToken()));
                context.write(friend, host);
            }
        }

    }

    public static class PairingReducer extends Reducer<IntWritable, IntWritable, Text, IntWritable> {

        private final Text pair = new Text();

        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            Set<Integer> hosts = new TreeSet<>();
            for (IntWritable val : values) {
                hosts.add(val.get());
            }
            for (int i = 0; i < hosts.size(); i++) {
                for (int j = i + 1; j < hosts.size(); j++) {
                    pair.set(hosts.toArray()[i].toString() + ", " + hosts.toArray()[j].toString());
                    context.write(pair, key);
                }
            }

        }

    }

    public static class FriendsRecordWriter extends RecordWriter<Text, Text> {

        FSDataOutputStream fos = null;

        public FriendsRecordWriter(TaskAttemptContext job) {
            FileSystem fs;
            try {
                fs = FileSystem.get(job.getConfiguration());
                Path outputPath = new Path("output/out.txt");
                fos = fs.create(outputPath);
            } catch (IOException e) {
                System.err.println("Caught exception while getting the configuration " + StringUtils.stringifyException(e));
            }
        }

        @Override
        public void write(Text key, Text value) throws IOException, InterruptedException {
            fos.write(("([" + key.toString() + "], [" + value.toString() + "])\n").getBytes());
        }

        @Override
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {
            IOUtils.closeStream(fos);
        }
    }

    public static class FriendsOutputFormat extends FileOutputFormat<Text, Text> {

        @Override
        public RecordWriter<Text, Text> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
            return new FriendsRecordWriter(job);
        }

    }

    public static class MergingReducer extends Reducer<Text, IntWritable, Text, Text> {

        private final Text friends = new Text();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            StringBuilder friendsString = new StringBuilder();
            boolean first = true;
            for (IntWritable val : values) {
                if (!first) {
                    friendsString.append(", ");
                } else {
                    first = false;
                }
                friendsString.append(val.toString());
            }
            friends.set(friendsString.toString());
            context.write(key, friends);
        }

    }

}