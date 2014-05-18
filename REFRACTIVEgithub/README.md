REFRACTIVE
==========

Refractive is a tool for extracting knowledge in the form of Frames from syntactic and semantic relations.

USAGE
=====
Refractive expects an input in the form of SequenceFileInputFormat<IntWritable, Text>. The sequencefile must contain integer-text pairs, where the text contains documents in CoNLL 2009 format.
To extract frames the following command can be used:
	
	> hadoop jar refractive.jar tool.frame.CoNLLFrameExtractor /path_to_input_sequencefiles /path_to_output number_of_reducers id_start id_end
	
This command will extract frames from the CoNLL documents found within the range id_start to id_end in the input sequencefiles.
Frame projections can then be created from the output files by using one of the tools found in the tool.statistics package.
For instance, to create a subject-verb-object projection, adding conditional probability statistics, the following command can be used:

	> hadoop jar refractive.jar tool.statistics.ConditionalProbabilityStatistics /path_to_frames /path_to_output number_of_reducers "SBJ,VERB,OBJ" "SBJ,VERB"

Finally, an export of the projected frames can be made by issuing the following:

	> java -jar exporttolucene.jar -index path_to_output -frames path_to_projected_frames

The java program tool.export.LuceneSearch contains an example of how to query the resulting Lucene index.
