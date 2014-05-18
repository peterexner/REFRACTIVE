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

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoubleField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

import edu.umd.cloud9.io.SequenceFileUtils;
import edu.umd.cloud9.io.pair.PairOfWritables;

public class ExportToLucene {
	public static void main(String[] args) throws IOException {
		String usage = "tool.export.ExportToLucene"
				+ " [-index INDEX_PATH] [-frames FRAMES_PATH]\n\n"
				+ "This indexes the frames in FRAMES_PATH, creating a Lucene index"
				+ "in INDEX_PATH";

		String indexPath = "index";
		String framesPath = null;
		for(int i=0;i<args.length;i++) {
			if ("-index".equals(args[i])) {
				indexPath = args[i+1];
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

		Directory dir = FSDirectory.open(new File(indexPath));
		Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_42);
		IndexWriterConfig iwc = new IndexWriterConfig(Version.LUCENE_42, analyzer);
		
		iwc.setOpenMode(OpenMode.CREATE);
		iwc.setRAMBufferSizeMB(3072.0);
		
		IndexWriter writer = new IndexWriter(dir, iwc);
		
		List<PairOfWritables<Text, DoubleWritable>> bigrams = SequenceFileUtils.readDirectory(new Path(framesPath));
		
		for (PairOfWritables<Text, DoubleWritable> bigram : bigrams) {
			String[] slots = bigram.getLeftElement().toString().trim().split("\t");
			double probability = bigram.getRightElement().get();

			Document doc = new Document();
			
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

					Field slotField = new TextField(slotRelation, slotValue.toString().substring(1, slotValue.toString().length() - 1), Field.Store.YES);
					doc.add(slotField);
				}	
			}

			Field probabilityField = new DoubleField("probability", probability, Field.Store.YES);
			doc.add(probabilityField);

			writer.addDocument(doc);
		}
		
		writer.forceMerge(1);
		writer.close();
	}
}