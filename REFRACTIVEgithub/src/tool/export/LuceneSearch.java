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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;

public class LuceneSearch {
	public static void main(String[] args) throws IOException, ParseException, org.apache.commons.cli.ParseException {
		Options options = new Options();

		options.addOption("index", true, "Lucene Index Path");
		options.addOption("subject", true, "Subject");
		options.addOption("verb", true, "Verb");
		options.addOption("object", true, "Object");

		CommandLineParser parser = new BasicParser();
		CommandLine cmd = parser.parse(options, args);
		
		if(cmd.hasOption("index") && cmd.getOptions().length > 1) {		
			IndexReader reader = DirectoryReader.open(FSDirectory.open(new File(cmd.getOptionValue("index"))));
			IndexSearcher searcher = new IndexSearcher(reader);


			BooleanQuery booleanQuery = new BooleanQuery();
			BooleanQuery booleanSubjectQuery = new BooleanQuery();
			BooleanQuery booleanVerbQuery = new BooleanQuery();
			BooleanQuery booleanObjectQuery = new BooleanQuery();
			if(cmd.hasOption("subject")) {
				booleanSubjectQuery.add(new TermQuery(new Term("SBJ-Y", cmd.getOptionValue("subject").toLowerCase() )), BooleanClause.Occur.SHOULD);
				booleanSubjectQuery.add(new TermQuery(new Term("SBJ-T", cmd.getOptionValue("subject").toLowerCase() )), BooleanClause.Occur.SHOULD);
				booleanQuery.add(booleanSubjectQuery, BooleanClause.Occur.MUST);
			}

			if(cmd.hasOption("verb")) {
				booleanVerbQuery.add(new TermQuery(new Term("VERB", cmd.getOptionValue("verb").toLowerCase() )), BooleanClause.Occur.MUST);
				booleanQuery.add(booleanVerbQuery, BooleanClause.Occur.MUST);
			}

			if(cmd.hasOption("object")) {
				booleanObjectQuery.add(new TermQuery(new Term("OBJ-Y", cmd.getOptionValue("object").toLowerCase() )), BooleanClause.Occur.SHOULD);
				booleanObjectQuery.add(new TermQuery(new Term("OBJ-T", cmd.getOptionValue("object").toLowerCase() )), BooleanClause.Occur.SHOULD);
				booleanQuery.add(booleanObjectQuery, BooleanClause.Occur.MUST);
			}


			Sort sort = new Sort(new SortField("probability", SortField.Type.DOUBLE, true));

			TopDocs hits = searcher.search(booleanQuery, null, 1000, sort);

			for(int i=0; i<hits.scoreDocs.length; i++) {
				Document document = searcher.doc(hits.scoreDocs[i].doc);
				for(IndexableField field:document.getFields()) {
					System.out.print(field.name() + ": " + field.stringValue() + "\t\t");
				}
				System.out.println();
			}
		} else {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp( "lucenesearch", options );	
		}
	}

}
