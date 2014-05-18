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

package tool.frame;

import io.Frame;
import java.io.IOException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import query.ProjectionQuery;


public class FrameProjector extends Configured implements Tool  {
	private static final Logger sLogger = Logger.getLogger(FrameProjector.class);

	public static class MapClass extends
	Mapper<LongWritable, Frame, LongWritable, Frame> {

		private static ProjectionQuery projectionQuery;
		
		@Override
		public void setup(Context context) {
			System.out.println("Setting up mapper...");
			projectionQuery = new ProjectionQuery(context.getConfiguration().get("projection"));
		}

		@Override
		public void map(LongWritable key, Frame frame, Context context)
				throws IOException,
				InterruptedException {
			
				Frame projectedFrame = projectionQuery.parseQuery(frame);
				if(projectedFrame != null) {
					context.getCounter("frame projection", "count").increment(1);
					context.write(key, projectedFrame);	
				}
			}
		}


	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 4) {
			System.out.println("Usage tool.frame.FrameProjector [inputPath] [outputPath] [numberOfReducers] [projection]");
			return -1;
		}

		String inputPath = args[0];
		String outPath = args[1];
		int reduceTasks = Integer.parseInt(args[2]);
		String projection = args[3];
		
		sLogger.info("Tool: Frame Projector");
		sLogger.info(" - input path: " + inputPath);
		sLogger.info(" - output path: " + outPath);
		sLogger.info(" - number of reducers: " + reduceTasks);
		sLogger.info(" - projection: " + projection);

		Job job = new Job(getConf(), "Frame Projector");

		job.getConfiguration().set("projection", projection);

		job.setJarByClass(FrameProjector.class);
		job.setNumReduceTasks(reduceTasks);

		FileInputFormat.addInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, new Path(outPath));

		job.setInputFormatClass(SequenceFileInputFormat.class);

		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Frame.class);

		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Frame.class);

		SequenceFileOutputFormat.setCompressOutput(job, false);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		job.setMapperClass(MapClass.class);
		
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
		int res = ToolRunner.run(new FrameProjector(), args);
		System.exit(res);
	}
}