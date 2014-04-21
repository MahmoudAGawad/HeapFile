package heap;
import java.io.*;
import java.util.*;

import diskmgr.*;
import bufmgr.*;

import global.*;
import heap.*;

public class PageArray {
	public static final int ROWS = 5, COLS = 5, SIZE = COLS * ROWS;
	private HFPage[][] arrayHfPage;
	private PriorityQueue<FreeSpace> maxFreeSpaces;
	private int next, maxFreeSpace;

	public PageArray() throws IOException, BufferPoolExceededException,
			HashOperationException, ReplacerException,
			HashEntryNotFoundException, InvalidFrameNumberException,
			PagePinnedException, PageUnpinnedException, PageNotReadException,
			BufMgrException, DiskMgrException {

		arrayHfPage = new HFPage[ROWS][COLS];
		Page header = new Page();
		SystemDefs.JavabaseBM.newPage(header, 1);
		maxFreeSpace = (arrayHfPage[0][0] = new HFPage(header))
				.available_space();
		next = 1;
		maxFreeSpaces = new PriorityQueue<>();
		maxFreeSpaces.add(new FreeSpace(maxFreeSpace, 0));

	}

	public RID addRecord(byte[] data, int retIndex) throws IOException,
			BufferPoolExceededException, HashOperationException,
			ReplacerException, HashEntryNotFoundException,
			InvalidFrameNumberException, PagePinnedException,
			PageUnpinnedException, PageNotReadException, BufMgrException,
			DiskMgrException { // called only when there is enough space

		FreeSpace suggested = maxFreeSpaces.peek();
		int key = suggested.getKey();
		int value = suggested.getValue();

		if (key >= data.length) {
			retIndex = value;
			maxFreeSpaces.poll();
			maxFreeSpaces.add(new FreeSpace(key - data.length, value));
			return arrayHfPage[value / ROWS][value % COLS].insertRecord(data);
		} else {
			retIndex = next;
			HFPage page = arrayHfPage[next / ROWS][next % COLS] = new HFPage();
			SystemDefs.JavabaseBM.newPage(page, 1);

			maxFreeSpaces.add(new FreeSpace(maxFreeSpace - data.length, next));
			next++;
			return page.insertRecord(data);
		}

	}

	public HFPage getPage(int index) {
		return arrayHfPage[index / ROWS][index % COLS];
	}
	
	public void updatePageSpace(int extra, int index){
		int newKey = extra; // + original
		for(FreeSpace node: maxFreeSpaces){
			if(node.getValue()==index){
				newKey += node.getKey(); 
				maxFreeSpaces.remove(node);
				break;
			}
		}
		
		// if newKey == maxFreeSpace, Should i not add this node?
		maxFreeSpaces.add(new FreeSpace(newKey, index));
	}

	public int getMaxFreeSpace() {
		return next < SIZE ? maxFreeSpace : maxFreeSpaces.peek().getKey();
	}
}
