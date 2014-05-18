/**
 * Refractive is a tool for extracting knowledge from syntactic and semantic relations.
 * Copyright © 2013 Peter Exner
 * 
 * This file is part of Refractive.
 *
 * Refractive is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Refractive is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Refractive.  If not, see <http://www.gnu.org/licenses/>.
 */

package tool.statistics;

import io.Frame;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import edu.umd.cloud9.io.map.String2IntOpenHashMapWritable;

import query.ProjectionQuery;


public class ConditionalProbabilityStatistics extends Configured implements Tool  {
	private static final Logger sLogger = Logger.getLogger(ConditionalProbabilityStatistics.class);

	public static class MapClass extends
	Mapper<LongWritable, Frame, Text, String2IntOpenHashMapWritable> {

		private static ProjectionQuery projectionQuery;
		private static ProjectionQuery conditionalProjectionQuery;
		private String2IntOpenHashMapWritable map;
		private static Text textKey = new Text();
		
		@Override
		public void setup(Context context) {
			System.out.println("Setting up mapper...");
			projectionQuery = new ProjectionQuery(context.getConfiguration().get("projection"));
			conditionalProjectionQuery = new ProjectionQuery(context.getConfiguration().get("conditional-projection"));
			map = new String2IntOpenHashMapWritable();
		}

		@Override
		public void map(LongWritable key, Frame frame, Context context)
				throws IOException,
				InterruptedException {

			Frame projectedFrame = projectionQuery.parseQuery(frame);
			Frame conditionalProjectedFrame = conditionalProjectionQuery.parseQuery(frame);
			
			if(conditionalProjectedFrame != null) {
				map.clear();
				map.put("*", 1);
				
				if(projectedFrame != null) {
					map.put(projectedFrame.toString(), 1);
				}
				
				textKey.set(conditionalProjectedFrame.toString());
				context.write(textKey, map);
			}
		}
	}

	public static class ReduceClass extends
	Reducer<Text, String2IntOpenHashMapWritable, Text, DoubleWritable> {
		private static Text text = new Text();
		private static DoubleWritable probability = new DoubleWritable();
		
		@Override
		public void setup(Context context) {
			System.out.println("Setting up reducer...");
		}

		@Override
		public void reduce(Text key, Iterable<String2IntOpenHashMapWritable> maps, Context context)
				throws IOException,
				InterruptedException {
			Iterator<String2IntOpenHashMapWritable> iter = maps.iterator();
			
			String2IntOpenHashMapWritable map = new String2IntOpenHashMapWritable();
			while (iter.hasNext()) {
				map.plus(iter.next());
			}
			
			double denominator = map.get("*");
			
			for(Entry<String, Integer> entry:map.entrySet()) {
				if(!entry.getKey().startsWith("*")) {
					text.set(entry.getKey());
					probability.set((double)entry.getValue() / denominator);
					context.getCounter("frame projection", "count").increment(1);
					context.write(text, probability);
				}
			}
		}
	}


	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 5) {
			System.out.println("Usage tool.statistics.ConditionalProbabilityStatistics [inputPath] [outputPath] [numberOfReducers] [target-projection] [conditional-projection]");
			return -1;
		}

		String inputPath = args[0];
		String outPath = args[1];
		int reduceTasks = Integer.parseInt(args[2]);
		String projection = args[3];
		String conditionalProjection = args[4];

		sLogger.info("Tool: ConditionalProbabilityStatistics");
		sLogger.info(" - input path: " + inputPath);
		sLogger.info(" - output path: " + outPath);
		sLogger.info(" - number of reducers: " + reduceTasks);
		sLogger.info(" - target-projection: " + projection);
		sLogger.info(" - conditional-projection: " + conditionalProjection);

		Job job = new Job(getConf(), " Conditional Probability Statistics");

		job.getConfiguration().set("projection", projection);
		job.getConfiguration().set("conditional-projection", conditionalProjection);

		job.setJarByClass(ConditionalProbabilityStatistics.class);
		job.setNumReduceTasks(reduceTasks);

		FileInputFormat.addInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, new Path(outPath));

		job.setInputFormatClass(SequenceFileInputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(String2IntOpenHashMapWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		SequenceFileOutputFormat.setCompressOutput(job, false);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		job.setMapperClass(MapClass.class);
		job.setReducerClass(ReduceClass.class);
		
		long startTime = System.currentTimeMillis();
		job.waitForCompletion(true);
		System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

		return 0;
	}

	/**
	 * Dispatches command-line arguments to the tool via the
	 * <code>ToolRunner</code>.
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new ConditionalProbabilityStatistics(), args);
		System.exit(res);
	}
}