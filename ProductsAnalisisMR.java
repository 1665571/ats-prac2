import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ProductsAnalisisMR {

    // MAPPER
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text pair = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString().trim();
            if (line.isEmpty()) return;

            String[] products = line.split("\\s+");

            // Generar todas las combinaciones de pares
            for (int i = 0; i < products.length; i++) {
                for (int j = i + 1; j < products.length; j++) {

                    String p1 = products[i];
                    String p2 = products[j];

                    // Ordenar para evitar (A,B) y (B,A)
                    List<String> pairList = new ArrayList<>();
                    pairList.add(p1);
                    pairList.add(p2);
                    Collections.sort(pairList);

                    pair.set(pairList.get(0) + "," + pairList.get(1));
                    context.write(pair, one);
                }
            }
        }
    }

    // REDUCER
    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            result.set(sum);
            context.write(key, result);
        }
    }

    // MAIN
    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println("Uso: ProductsAnalisisMR <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Product Co-occurrence");

        job.setJarByClass(ProductsAnalisisMR.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}