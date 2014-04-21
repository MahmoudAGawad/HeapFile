package heap;
import java.io.IOException;

import bufmgr.BufMgrException;
import bufmgr.BufferPoolExceededException;
import bufmgr.HashEntryNotFoundException;
import bufmgr.HashOperationException;
import bufmgr.InvalidFrameNumberException;
import bufmgr.PageNotReadException;
import bufmgr.PagePinnedException;
import bufmgr.PageUnpinnedException;
import bufmgr.ReplacerException;

import global.*;
import heap.*;

public class Scan {
	private RID rid;
	private Heapfile hf;
	private HFPage hfPage;
	private boolean scanClosed;
	private PageId lastId;

	public Scan(Heapfile hf) throws IOException {
		this.hf = hf;
		hfPage = hf.getHeaderHfPage();
		rid = hfPage.firstRecord();
		System.out.println("First Record Pageid = "+rid.pageNo);
		scanClosed = false;
	}

	public Tuple getNext(RID rid) throws IOException,
			InvalidSlotNumberException, ReplacerException,
			HashOperationException, PageUnpinnedException,
			InvalidFrameNumberException, PageNotReadException,
			BufferPoolExceededException, PagePinnedException, BufMgrException,
			HashEntryNotFoundException {
		/*
		 * if we have the first record id in the heapfile we should return it in
		 * the first case
		 */
		
		if(scanClosed){
			if(hfPage != null){
				hfPage=null;
				SystemDefs.JavabaseBM.unpinPage(lastId, true);
			}
			closescan();
			return null;
		}
		
		if(hfPage==null||this.rid==null){
			return null;
		}
		
		
		rid.copyRid(this.rid);
		Tuple tuple = hfPage.getRecord(rid);
		
		
		
		this.rid = hfPage.nextRecord(rid);
		if (this.rid == null) {
			
			PageId pageId = hfPage.getNextPage();
			if (pageId.pid == -1) {
				scanClosed = true;
				lastId = rid.pageNo;
				return tuple;
			}
			SystemDefs.JavabaseBM.unpinPage(rid.pageNo, true);
			SystemDefs.JavabaseBM.pinPage(pageId, hfPage, false);
			this.rid = hfPage.firstRecord();
			
		}

		return tuple;
	}

	public boolean position(RID rid) throws InvalidSlotNumberException,
			ReplacerException, HashOperationException, PageUnpinnedException,
			InvalidFrameNumberException, PageNotReadException,
			BufferPoolExceededException, PagePinnedException, BufMgrException,
			HashEntryNotFoundException, IOException {
		if (hf.heapContains(rid)) {
			this.rid = rid;
			return true;
		}
		rid = null;
		return false;
	}

	public void closescan() {
		rid = null;
		hfPage = null;
	}

}
