import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

import java.io.IOException;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.StringTokenizer;

public class InvertedIndex {
    public static class InvertedIndexMapper
        extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable intValue = new IntWritable();
        private final static Text textKey = new Text();

        @Override
        protected void map(Object key, Text value, Context content)
                throws IOException, InterruptedException {

            FileSplit fileSplit = (FileSplit)content.getInputSplit();
            String fileName = fileSplit.getPath().getName().split("(.txt)|(.TXT)")[0];

            /* Count the frequency of words that appear in a line. */
            Hashtable ht = new Hashtable<String, Integer>();
            StringTokenizer itr = new StringTokenizer(value.toString());
            while(itr.hasMoreTokens()){
                String word = itr.nextToken();
                if(ht.containsKey(word)){
                    ht.put(word, new Integer(((Integer)ht.get(word)).intValue()+1));
                }
                else{
                    ht.put(word, new Integer(1));
                }
            }

            Enumeration<String> words = ht.keys();
            while(words.hasMoreElements()){
                String word = words.nextElement();
                textKey.set(word+","+fileName);
                intValue.set(((Integer)ht.get(word)).intValue());
                content.write(textKey, intValue);
            }
        }
    }

    public static class LineCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable intValue = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

            /* Count the frequency of words that appear in a file. */
            int sum = 0;
            for(IntWritable value : values)
                sum += value.get();

            intValue.set(sum);
            context.write(key, intValue);
        }
    }

    public static class WordPartition extends HashPartitioner<Text, Object> {

        private final static Text word = new Text();
        @Override
        public int getPartition(Text key, Object value, int numReduceTasks) {
            word.set(key.toString().split(",")[0]);
            return super.getPartition(word, value, numReduceTasks);
        }
    }

    public static class InvertedIndexReducer extends Reducer<Text, IntWritable, Text, Text> {

        private static String prevWord = null;
        private static int sum_of_frequency = 0;     // sum of frequency of words in all files
        private static int num_of_file = 0;         //  num of files containing the word
        StringBuilder postings = new StringBuilder(); //format : filename1:frequency1;...

        private final static Text textKey = new Text();
        private final static Text textValue = new Text();
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            String[] word_filename = key.toString().split(",");
            if(prevWord!=null && !word_filename[0].equals(prevWord)){       // New word
                textKey.set(prevWord);
                float average = (float) sum_of_frequency / num_of_file ;
                textValue.set(average+","+postings.toString());
                context.write(textKey, textValue);

                sum_of_frequency = num_of_file = 0;
                postings.delete(0, postings.length());
            }
            int num_of_frequency = 0;   // num of frequency of word in this file
            for(IntWritable value : values)     // values.length() == 1 ?
                num_of_frequency += value.get();
            num_of_file ++ ;
            sum_of_frequency += num_of_frequency;
            if(num_of_file != 1)
                postings.append(";");
            postings.append(word_filename[1]+":"+num_of_frequency);

            prevWord = word_filename[0];
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            textKey.set(prevWord);
            float average = (float) sum_of_frequency / num_of_file ;
            textValue.set(average+","+postings.toString());
            context.write(textKey, textValue);
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "inverted index");

        job.setJarByClass(InvertedIndex.class);

        job.setMapperClass(InvertedIndexMapper.class);
        job.setCombinerClass(LineCombiner.class);
        job.setPartitionerClass(WordPartition.class);
        job.setReducerClass(InvertedIndexReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
