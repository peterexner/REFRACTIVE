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

import id.HadoopUniqueIdGenerator;
import io.Frame;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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

import conll.model.Document;
import conllentity.io.CoNLLEntityReader;
import extract.FrameExtractor;


public class CoNLLFrameExtractor extends Configured implements Tool  {
	private static final Logger sLogger = Logger.getLogger(CoNLLFrameExtractor.class);

	public static class MapClass extends
	Mapper<IntWritable, Text, IntWritable, Text> {

		private int startId;
		private int endId;

		@Override
		public void setup(Context context) {
			System.out.println("Setting up...");
			startId = context.getConfiguration().getInt("startId", -1);
			endId = context.getConfiguration().getInt("endId", Integer.MAX_VALUE);
		}

		@Override
		public void map(IntWritable key, Text text, Context context) throws IOException,
		InterruptedException {
			if(key.get() < startId) {
				return;
			}

			if(key.get() > endId) {
				return;
			}

			context.write(key, text);
		}
	}



	public static class ReduceClass extends
	Reducer<IntWritable, Text, LongWritable, Frame> {

		private static CoNLLEntityReader coNLLEntityReader;
		private static LongWritable frameId = new LongWritable();

		@Override
		public void setup(Context context) {
			System.out.println("Setting up reducer...");
			coNLLEntityReader = new CoNLLEntityReader();
			FrameExtractor.setUniqueIdGenerator(new HadoopUniqueIdGenerator(context.getCounter("frame", "id")));
		}

		@Override
		public void reduce(IntWritable key, Iterable<Text> texts, Context context)
				throws IOException,
				InterruptedException {
			Iterator<Text> iter = texts.iterator();

			while (iter.hasNext()) {
				Document document = coNLLEntityReader.readString(iter.next().toString(), Charset.forName("UTF-8"));
				
				List<Frame> frames;
				try {
					frames = FrameExtractor.extractFrames(document, 2, context);

					for(Frame frame:frames) {
						frameId.set(frame.getFrameId());
						context.write(frameId, frame);
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 5) {
			System.out.println("Usage tool.Frame.CoNLLFrameExtractor [inputPath] [outputPath] [numberOfReducers] [startId] [endId]");
			return -1;
		}

		String inputPath = args[0];
		String outPath = args[1];
		int reduceTasks = Integer.parseInt(args[2]);
		int startId = Integer.parseInt(args[3]);
		int endId = Integer.parseInt(args[4]);

		sLogger.info("Tool: CoNLL Frame Extractor");
		sLogger.info(" - input path: " + inputPath);
		sLogger.info(" - output path: " + outPath);
		sLogger.info(" - number of reducers: " + reduceTasks);
		sLogger.info(" - starting document id: " + startId);
		sLogger.info(" - ending document id: " + endId);

		Job job = new Job(getConf(), "CoNLL Frame Extractor");

		job.getConfiguration().setInt("startId", startId);
		job.getConfiguration().setInt("endId", endId);

		job.getConfiguration().setLong("mapred.task.timeout", 60*60*1000);

		job.setJarByClass(CoNLLFrameExtractor.class);
		job.setNumReduceTasks(reduceTasks);

		FileInputFormat.addInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, new Path(outPath));

		job.setInputFormatClass(SequenceFileInputFormat.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Frame.class);

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
		int res = ToolRunner.run(new CoNLLFrameExtractor(), args);
		System.exit(res);
	}
}