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

package tool.export;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.lucene.document.DoubleField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;

import edu.umd.cloud9.io.SequenceFileUtils;
import edu.umd.cloud9.io.pair.PairOfWritables;

public class ExportToNTriples {
	public static void main(String[] args) throws IOException {
		String usage = "tool.export.ExportToNTriples"
				+ " [-triples TRIPLES_PATH] [-frames FRAMES_PATH]\n\n"
				+ "This converts the frames in FRAMES_PATH to triples in NTriple format in INDEX_PATH.";

		String triplePath = "triples";
		String framesPath = null;
		for(int i=0;i<args.length;i++) {
			if ("-triples".equals(args[i])) {
				triplePath = args[i+1];
				i++;
			} else if ("-frames".equals(args[i])) {
				framesPath = args[i+1];
				i++;
			}
		}

		if (framesPath == null) {
			System.err.println("Usage: " + usage);
			System.exit(1);
		}

		BufferedWriter out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(triplePath), Charset.forName("UTF-8")));

		List<PairOfWritables<Text, DoubleWritable>> bigrams = SequenceFileUtils.readDirectory(new Path(framesPath));
		
		for (PairOfWritables<Text, DoubleWritable> bigram : bigrams) {
			StringBuilder sb = new StringBuilder();
			String[] slots = bigram.getLeftElement().toString().trim().split("\t");
			double probability = bigram.getRightElement().get();
			
			for(int i=0; i<slots.length; i++) {
				String slot = slots[i];
				if(i==0) {
					slot = slot.substring(2, slot.length() - 1);
				} else {
					slot = slot.substring(1, slot.length() - 1);
				}

				String[] slotParts = slot.split(",");
				if(slotParts.length >= 3) {
					String slotRelation = slotParts[0];
					StringBuilder slotValue = new StringBuilder();

					String separator = "";
					for(int j=1; j<slotParts.length-1; j++) {
						slotValue.append(separator);
						slotValue.append(slotParts[j]);
						separator = ",";
					}

					sb.append("<" + slotValue.toString().substring(1, slotValue.toString().length() - 1) + "> ");
				}	
			}

			out.write(sb.toString() + ".\n");
		}
		
		out.flush();
		out.close();
	}
}