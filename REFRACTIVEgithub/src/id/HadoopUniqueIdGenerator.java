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

package id;

import org.apache.hadoop.mapreduce.Counter;

public class HadoopUniqueIdGenerator extends UniqueIdGenerator {
	private Counter counter;
	
	public HadoopUniqueIdGenerator(Counter counter) {
		this.counter = counter;
	}
	
	@Override
	public void setSeed(long seed) {
		counter.setValue(seed);
	}

	@Override
	public long nextLong() {
		long nextLong = counter.getValue();
		counter.increment(1);
		return nextLong;
	}

}
