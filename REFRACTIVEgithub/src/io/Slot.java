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

import org.apache.hadoop.io.Writable;

public class Slot implements Writable {
	private String relation;
	private String value;
	private Boolean isProperNoun;
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("<");
		sb.append(this.relation);
		sb.append(",");
		sb.append("\"");
		sb.append(this.value);
		sb.append("\"");
		if(this.isProperNoun) {
			sb.append(",Y");
		} else {
			sb.append(",N");
		}
		sb.append(">");

		return sb.toString();
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.relation = in.readUTF();
		this.value = in.readUTF();
		this.isProperNoun = in.readBoolean();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(this.relation);
		out.writeUTF(this.value);
		out.writeBoolean(this.isProperNoun);
	}

	public String getRelation() {
		return relation;
	}

	public void setRelation(String relation) {
		this.relation = relation;
	}
	
	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public Boolean getIsProperNoun() {
		return isProperNoun;
	}

	public void setIsProperNoun(Boolean isProperNoun) {
		this.isProperNoun = isProperNoun;
	}
}
