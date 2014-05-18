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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import io.Frame;
import io.Slot;

public class ProjectionQuery {
	List<SlotQuery> slotQueryList;
	
	public ProjectionQuery(String query) {
		this.parseQueryString(query);
	}
	
	public Frame parseQuery(Frame frame) {
		if(slotQueryList.size() == 0) {
			return null;
		}
		
		Iterator<Slot> slotIterator = frame.getSlots().iterator();
		Iterator<SlotQuery> slotQueryIterator = slotQueryList.iterator();
		Slot slot;
		SlotQuery slotQuery;
		
		if(slotQueryIterator.hasNext()) {
			slotQuery = slotQueryIterator.next();
		} else {
			return null;
		}
		
		Frame projectedFrame = new Frame();
		projectedFrame.setFrameId(frame.getFrameId());
		
		while(slotIterator.hasNext() && (this.slotQueryList.size() != projectedFrame.getSlots().size())) {
			slot = slotIterator.next();
			
			if(slotQuery.match(slot)) {
				projectedFrame.getSlots().add(slot);
				
				if(slotQueryIterator.hasNext()) {
					slotQuery = slotQueryIterator.next();
				}
			}
		}
		
		if(this.slotQueryList.size() == projectedFrame.getSlots().size()) {
			return projectedFrame;
		} else {
			return null;
		}			
	}
	
	private void parseQueryString(String query) {
		this.slotQueryList = new ArrayList<SlotQuery>();
		
		String[] slotQueries = query.split(",");
		for(int i=0; i<slotQueries.length; i++) {
			SlotQuery slotQuery = new SlotQuery(slotQueries[i]);
			slotQueryList.add(slotQuery);
		}
	}
}
