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

import query.ProjectionQuery;


public class FrequencyStatistics extends Configured implements Tool  {
	private static final Logger sLogger = Logger.getLogger(FrequencyStatistics.class);

	public static class MapClass extends
	Mapper<LongWritable, Frame, Text, Frame> {

		private static ProjectionQuery projectionQuery;
		private static Text textKey = new Text();
		
		@Override
		public void setup(Context context) {
			System.out.println("Setting up mapper...");
			projectionQuery = new ProjectionQuery(context.getConfiguration().get("query"));
		}

		@Override
		public void map(LongWritable key, Frame frame, Context context)
				throws IOException,
				InterruptedException {

			Frame projectedFrame = projectionQuery.parseQuery(frame);
			if(projectedFrame != null) {
				textKey.set(projectedFrame.toSlotValues());
				context.write(textKey, projectedFrame);	
			}
		}
	}

	public static class ReduceClass extends
	Reducer<Text, Frame, Text, DoubleWritable> {
		private static Text text = new Text();
		private static DoubleWritable count = new DoubleWritable();
		
		@Override
		public void setup(Context context) {
			System.out.println("Setting up reducer...");
		}

		@Override
		public void reduce(Text key, Iterable<Frame> frames, Context context)
				throws IOException,
				InterruptedException {
			Iterator<Frame> iter = frames.iterator();
			
			Frame frame = null;
			long frameCount = 0;
			
			while (iter.hasNext()) {
				frame = iter.next();
				frameCount++;
			}
			
			if(frameCount > 0) {
				text.set(frame.toString());
				count.set(frameCount);
				context.getCounter("frame projection", "count").increment(1);
				context.write(text, count);
			}
		}
	}


	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 4) {
			System.out.println("Usage tool.statistics.FrequencyStatistics [inputPath] [outputPath] [numberOfReducers] [projection]");
			return -1;
		}

		String inputPath = args[0];
		String outPath = args[1];
		int reduceTasks = Integer.parseInt(args[2]);
		String query = args[3];

		sLogger.info("Tool: FrequencyStatistics");
		sLogger.info(" - input path: " + inputPath);
		sLogger.info(" - output path: " + outPath);
		sLogger.info(" - number of reducers: " + reduceTasks);
		sLogger.info(" - projection: " + query);

		Job job = new Job(getConf(), "Frequency Statistics");

		job.getConfiguration().set("query", query);

		job.setJarByClass(FrequencyStatistics.class);
		job.setNumReduceTasks(reduceTasks);

		FileInputFormat.addInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, new Path(outPath));

		job.setInputFormatClass(SequenceFileInputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Frame.class);

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
		int res = ToolRunner.run(new FrequencyStatistics(), args);
		System.exit(res);
	}
}