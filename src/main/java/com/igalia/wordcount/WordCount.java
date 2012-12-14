/*
 * Copyright (C) 2012 Igalia, S.L.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.igalia.wordcount;

import static org.apache.hadoop.hbase.util.Bytes.toBytes;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;

/**
 * 
 * How to write output of a job to a HBase database
 * 
 * @author Diego Pino García <dpino@igalia.com>
 * 
 */
public class WordCount extends Configured implements Tool {
	
	private static final String INPUT_TABLE = "files";
	private static final String OUTPUT_TABLE = "words";
	
	public static class MapClass extends TableMapper<Text, IntWritable> {

		private static final IntWritable ONE = new IntWritable(1);
		private Text word = new Text();
		
		@Override
		protected void map(ImmutableBytesWritable key, Result row,
				Context context) throws IOException, InterruptedException {

			String content = Bytes.toString(row.getValue(
					Bytes.toBytes("content"), Bytes.toBytes("")));

			StringTokenizer tokenizer = new StringTokenizer(content);
			while (tokenizer.hasMoreTokens()) {
				String token = tokenizer.nextToken();
				word.set(token);
				context.write(word, ONE);
			}
			
		}

	}
		
	public static class Reduce extends
			TableReducer<Text, IntWritable, Text> {

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {

			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}
			Put put = new Put(toBytes(key.toString()));
			put.add(toBytes("number"), toBytes(""), toBytes(sum));
			context.write(null, put);
		}
	}

	public int run(String[] arg0) throws Exception {		
        Job job = new Job(getConf());
		job.setJarByClass(WordCount.class);
		job.setJobName("wordcount");
						
	    Scan scan = new Scan();
        scan.setCaching(500);
        scan.setCacheBlocks(false);
		
        TableMapReduceUtil.initTableMapperJob(
                INPUT_TABLE,
                scan,
                WordCount.MapClass.class,
                Text.class, IntWritable.class,
                job);        
        TableMapReduceUtil.initTableReducerJob(
        		OUTPUT_TABLE,
                WordCount.Reduce.class,
                job);        

		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1; 
	}

}
