import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Copyright 2012-2013 The Regents of the University of California
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on
 * an "AS IS"; BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under
 * the License.
 * 
 * Author: maha alabduljalil <maha (at) cs.ucsb.edu>
 * @Since Feb 12, 2013
 */

/**
 * @author Maha
 * 
 */
public class Reduce extends Reducer<Text, CompositeWritable, Text, CompositeWritable> {

	@Override
	public void reduce(Text key, Iterable<CompositeWritable> values, Context context) throws IOException,
			InterruptedException {
		CompositeWritable result = new CompositeWritable(0, 0, 0);
		for (CompositeWritable val : values) {
			result.merge(val);
		}
		context.write(key, result);
	}
}
