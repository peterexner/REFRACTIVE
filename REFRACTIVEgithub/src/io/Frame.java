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

package io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Writable;

public class Frame implements Writable {
	private long frameId;
	private List<Slot> slots;

	public Frame() {
		this.slots = new ArrayList<Slot>();
	}
	
	public long getFrameId() {
		return frameId;
	}

	public void setFrameId(long frameId) {
		this.frameId = frameId;
	}
	
	public List<Slot> getSlots() {
		return slots;
	}

	public void setSlots(List<Slot> slots) {
		this.slots = slots;
	}
		
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		String separator = ""; 
		
		sb.append("{");
		
		for(Slot slot:slots) {
			sb.append(separator);
			sb.append(slot.toString());
			separator = "\t";
		}
		
		sb.append("}");
		
		return sb.toString();
	}
	
	public String toSlotValues() {
		StringBuilder sb = new StringBuilder();
		String separator = ""; 
		
		for(Slot slot:slots) {
			sb.append(separator);
			sb.append(slot.getValue());
			separator = " ";
		}
		
		return sb.toString();
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		slots.clear();
		
		int numberOfSlots = in.readInt();
		
		Slot slot;
		for(int i=0; i<numberOfSlots; i++) {
			slot = new Slot();
			slot.readFields(in);
			slots.add(slot);
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(slots.size());
		
		for(Slot slot:slots) {
			slot.write(out);
		}
	}
}
