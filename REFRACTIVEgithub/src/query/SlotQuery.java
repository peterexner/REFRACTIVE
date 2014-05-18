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

package query;

import io.Slot;

public class SlotQuery {
	String targetRelation;
	boolean requireProperNoun;
	
	public SlotQuery(String query) {
		String[] queryParts = query.trim().split(":");
		if(queryParts.length == 2) {
			this.targetRelation = queryParts[0].trim();
			
			if(queryParts[1].trim().equalsIgnoreCase("Y")) {
				requireProperNoun = true;
			} else {
				requireProperNoun = false;
			}
		} else {
			this.targetRelation = query.trim();
			requireProperNoun = false;
		} 
	}
	
	public boolean match(Slot slot) {
		if(this.requireProperNoun) {
			return slot.getRelation().equalsIgnoreCase(targetRelation) && slot.getIsProperNoun();
		} else {
			return slot.getRelation().equalsIgnoreCase(targetRelation);
		}
	}
}
