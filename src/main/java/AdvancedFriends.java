import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeSet;

public class AdvancedFriends {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        GenericOptionsParser optionsParser = new GenericOptionsParser(conf, args);
        String[] remainingArgs = optionsParser.getRemainingArgs();

        Path tempPath = new Path("temp");

        Job job1 = Job.getInstance(conf, "transposition & pairing");
        job1.setJarByClass(AdvancedFriends.class);
        job1.setMapperClass(TranspositionMapper.class);
        job1.setMapOutputKeyClass(IntWritable.class);
        job1.setMapOutputKeyClass(IntWritable.class);
        job1.setNumReduceTasks(1);
        job1.setReducerClass(PairingReducer.class);
        job1.setOutputKeyClass(IntSet.class);
        job1.setOutputValueClass(IntWritable.class);
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job1, new Path(remainingArgs[0]));
        FileOutputFormat.setOutputPath(job1, tempPath);

        if (!job1.waitForCompletion(true)) System.exit(1);

        Job job2 = Job.getInstance(conf, "merging");
        job2.setJarByClass(AdvancedFriends.class);
        job2.setInputFormatClass(SequenceFileInputFormat.class);
        job2.setMapOutputKeyClass(IntSet.class);
        job2.setMapOutputValueClass(IntWritable.class);
        job2.setNumReduceTasks(1);
        job2.setReducerClass(MergingReducer.class);
        job2.setOutputKeyClass(IntSet.class);
        job2.setOutputValueClass(IntWritable.class);
        job2.setOutputFormatClass(FriendsOutputFormat.class);

        FileInputFormat.addInputPath(job2, tempPath);
        FileOutputFormat.setOutputPath(job2, new Path(remainingArgs[1]));

        if (!job2.waitForCompletion(true)) System.exit(1);

        System.exit(0);

    }

    public static class IntSet extends TreeSet<Integer> implements WritableComparable {

        @Override
        public int compareTo(Object o) {
            for (int i = 0; i < this.size(); i++) {
                if (i >= ((IntSet) o).size()) return 1;
                if (((Integer) this.toArray()[i]).compareTo((Integer) ((IntSet) o).toArray()[i]) > 0) return 1;
                else if (((Integer) this.toArray()[i]).compareTo((Integer) ((IntSet) o).toArray()[i]) < 0) return -1;
            }
            if (this.size() == ((IntSet) o).size()) return 0;
            else return -1;
        }

        @Override
        public boolean equals(Object o) {
            return compareTo(o) == 0;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(this.size());
            for (Integer i : this) {
                out.writeInt(i);
            }
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            this.clear();
            int s = in.readInt();
            for (int i = 0; i < s; i++) {
                int t = in.readInt();
                this.add(t);
            }
        }

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

    public static class PairingReducer extends Reducer<IntWritable, IntWritable, IntSet, IntWritable> {

        private final IntSet pair = new IntSet();

        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            Set<Integer> hosts = new TreeSet<>();
            for (IntWritable val : values) {
                hosts.add(val.get());
            }
            for (int i = 0; i < hosts.size(); i++) {
                for (int j = i + 1; j < hosts.size(); j++) {
                    pair.clear();
                    pair.add((Integer) hosts.toArray()[i]);
                    pair.add((Integer) hosts.toArray()[j]);
                    context.write(pair, key);
                }
            }

        }

    }

    public static class FriendsRecordWriter extends RecordWriter<IntSet, IntSet> {

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
        public void write(IntSet key, IntSet value) throws IOException, InterruptedException {
            fos.write("([".getBytes());
            boolean first = true;
            for (Integer i : key) {
                if (!first) {
                    fos.write(", ".getBytes());
                } else {
                    first = false;
                }
                fos.write(i.toString().getBytes());
            }
            fos.write("], [".getBytes());
            first = true;
            for (Integer i : value) {
                if (!first) {
                    fos.write(", ".getBytes());
                } else {
                    first = false;
                }
                fos.write(i.toString().getBytes());
            }
            fos.write("])\n".getBytes());
        }

        @Override
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {
            IOUtils.closeStream(fos);
        }
    }

    public static class FriendsOutputFormat extends FileOutputFormat<IntSet, IntSet> {

        @Override
        public RecordWriter<IntSet, IntSet> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
            return new FriendsRecordWriter(job);
        }

    }

    public static class MergingReducer extends Reducer<IntSet, IntWritable, IntSet, IntSet> {

        private final IntSet friends = new IntSet();

        @Override
        public void reduce(IntSet key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            friends.clear();
            for (IntWritable val : values) {
                friends.add(val.get());
            }
            context.write(key, friends);
        }

    }

}