package heap;

import java.io.*;
import java.util.*;

import bufmgr.*;
import diskmgr.*;

import global.*;
import heap.*;

public class Heapfile implements GlobalConst {

	private Hashtable<Integer, Integer> pageIds;
	private int records;
	private HFPage headerPage;
	private PageId headerPageId;
	private String fileName;

	public Heapfile(String name) throws OutOfSpaceException,
			InvalidRunSizeException, BufferPoolExceededException,
			HashOperationException, ReplacerException,
			HashEntryNotFoundException, InvalidFrameNumberException,
			PagePinnedException, PageUnpinnedException, PageNotReadException,
			BufMgrException, FileNameTooLongException, DuplicateEntryException, InvalidSlotNumberException {

		pageIds = new Hashtable<>();
		records = 0;
		headerPage = new HFPage();
		fileName = name;
		try {
			headerPageId = SystemDefs.JavabaseDB.get_file_entry(name);
			if (headerPageId == null) {
				
				headerPageId = SystemDefs.JavabaseBM.newPage(headerPage, 1);

				SystemDefs.JavabaseDB.add_file_entry(name, headerPageId);

				SystemDefs.JavabaseBM.unpinPage(headerPageId, true);
				headerPage.init(headerPageId, headerPage);
				pageIds.put(headerPageId.pid, 1);
			} else {
				SystemDefs.JavabaseBM.pinPage(headerPageId, headerPage, false);
				SystemDefs.JavabaseBM.unpinPage(headerPageId, false);
				
				
				Scan scan = openScan();
				RID rid =new RID();
				Tuple tp = scan.getNext(rid);
				while(tp!=null){
					records++;
					if(pageIds.containsKey(rid.pageNo.pid)){
						pageIds.put(rid.pageNo.pid, (int)pageIds.get(rid.pageNo.pid)+1);
					}else{
						pageIds.put(rid.pageNo.pid, 1);
					}
					tp = scan.getNext(rid);
					
				}
				System.out.println("How Many : "+pageIds.get(2));
				scan.closescan();
			}

			
			
		} catch (FileIOException | InvalidPageNumberException
				| DiskMgrException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public int getRecCnt() {
		return records;
	}

	public boolean heapContains(RID rid) throws InvalidSlotNumberException,
			ReplacerException, HashOperationException, PageUnpinnedException,
			InvalidFrameNumberException, PageNotReadException,
			BufferPoolExceededException, PagePinnedException, BufMgrException,
			HashEntryNotFoundException, IOException {
		return getRecord(rid) == null ? false : true;
	}

	public HFPage getHeaderHfPage() {
		return headerPage;
	}

	public RID insertRecord(byte recPtr[]) throws IOException,
			BufferPoolExceededException, HashOperationException,
			ReplacerException, HashEntryNotFoundException,
			InvalidFrameNumberException, PagePinnedException,
			PageUnpinnedException, PageNotReadException, BufMgrException,
			DiskMgrException, SpaceNotAvailableException {
		
		if(recPtr.length>MINIBASE_PAGESIZE){
			throw(new SpaceNotAvailableException(null, null));
		}
		
		System.out.print("->Insert Record in: ");
		PageId currentPageId = headerPage.getCurPage();
		HFPage temp = new HFPage();
		int i = 0;
		while (currentPageId.pid != -1) {
			i++;
			SystemDefs.JavabaseBM.pinPage(currentPageId, temp, false);
			
			if (temp.available_space() >= recPtr.length) {
				// found
				records++;
				System.out.println("Already Existed Page #" + i);
				RID ret = temp.insertRecord(recPtr);
				SystemDefs.JavabaseBM.unpinPage(currentPageId, true);
				System.out.println("# of Recordes = "+records);
				return ret;
			}
			SystemDefs.JavabaseBM.unpinPage(currentPageId, true);
			currentPageId = temp.getNextPage();
		}

		HFPage newHfPage = new HFPage();
		currentPageId = SystemDefs.JavabaseBM.newPage(newHfPage, 1);
		temp.setNextPage(currentPageId);
		newHfPage.setPrevPage(temp.getCurPage());

		newHfPage.init(currentPageId, newHfPage);

		if (pageIds.containsKey(currentPageId.pid)) {
			pageIds.put(currentPageId.pid, pageIds.get(currentPageId.pid)+1); // existed
																			// pageid
		} else {
			pageIds.put(currentPageId.pid, 1); // new pageid
		}
		records++;
		RID ret=newHfPage.insertRecord(recPtr);
		SystemDefs.JavabaseBM.unpinPage(currentPageId, true);
		System.out.println("a new Page");
		System.out.println("# of Recordes = "+records);
		return ret;
	}

	public boolean deleteRecord(RID rid) throws IOException, ReplacerException,
			HashOperationException, PageUnpinnedException,
			InvalidFrameNumberException, PageNotReadException,
			BufferPoolExceededException, PagePinnedException, BufMgrException,
			HashEntryNotFoundException, InvalidSlotNumberException {
		
		if (!pageIds.containsKey(rid.pageNo.pid)) {
			System.out.println("Delettion is unsuccessful!");
			return false; // not found
		}
		HFPage find = new HFPage();
		SystemDefs.JavabaseBM.pinPage(rid.pageNo, find, false);
		RID curRid = find.firstRecord();
		while (curRid != null) {
			if (curRid.equals(rid)) {
				// found
				find.deleteRecord(rid);
				SystemDefs.JavabaseBM.unpinPage(rid.pageNo, true);
				records--;
				
				if (pageIds.get(rid.pageNo.pid) == 1) {
					pageIds.remove(rid.pageNo.pid);

//					if (find.getNextPage() == null
//							&& find.getPrevPage() == null) {
//
//					} else if (find.getNextPage() == null) {
//						find.setCurPage(find.getPrevPage());
//						find.setNextPage(null);
//					} else if (find.getPrevPage() == null) {
//						headerPage.setCurPage(headerPageId=find.getNextPage());
//						headerPage.setPrevPage(null);
//					} else {
//						HFPage prev = new HFPage();
//						SystemDefs.JavabaseBM.pinPage(find.getPrevPage(), prev,
//								false);
//						SystemDefs.JavabaseBM.unpinPage(find.getPrevPage(),
//								false);
//						HFPage next = new HFPage();
//						SystemDefs.JavabaseBM.pinPage(find.getNextPage(), next,
//								false);
//						SystemDefs.JavabaseBM.unpinPage(find.getNextPage(),
//								false);
//						prev.setNextPage(next.getCurPage());
//						next.setPrevPage(prev.getCurPage());
//						find.setNextPage(null);
//						find.setPrevPage(null);
//					}

				} else {
					pageIds.put(rid.pageNo.pid, pageIds.get(rid.pageNo.pid) - 1);
				}
				return true;
			}
			curRid = find.nextRecord(curRid);
		}
		
		SystemDefs.JavabaseBM.unpinPage(rid.pageNo, true);
		return false;
	}

	public boolean updateRecord(RID rid, Tuple newtuple)
			throws InvalidSlotNumberException, IOException, ReplacerException,
			HashOperationException, PageUnpinnedException,
			InvalidFrameNumberException, PageNotReadException,
			BufferPoolExceededException, PagePinnedException, BufMgrException,
			HashEntryNotFoundException, InvalidUpdateException {

		if (!pageIds.containsKey(rid.pageNo.pid)) {
			return false; // not found
		}

		HFPage find = new HFPage();
		SystemDefs.JavabaseBM.pinPage(rid.pageNo, find, false);
		

		RID curRid = find.firstRecord();
		while (curRid != null) {
			if (curRid.equals(rid)) {
				// found
				Tuple orignalTuple = find.returnRecord(curRid);
				
				if (orignalTuple.getLength() == newtuple.getLength()) {
//					System.out.println(newtuple.getLength());
					orignalTuple.tupleCopy(newtuple);
					SystemDefs.JavabaseBM.unpinPage(rid.pageNo, true);
					return true;
				} else {
					SystemDefs.JavabaseBM.unpinPage(rid.pageNo, false);
					throw(new InvalidUpdateException(null,null));
//					return false;
				}
			}
			curRid = find.nextRecord(curRid);
		}
		
		SystemDefs.JavabaseBM.unpinPage(rid.pageNo, false);
		return false;
	}

	public Tuple getRecord(RID rid) throws InvalidSlotNumberException,
			IOException, ReplacerException, HashOperationException,
			PageUnpinnedException, InvalidFrameNumberException,
			PageNotReadException, BufferPoolExceededException,
			PagePinnedException, BufMgrException, HashEntryNotFoundException {

		if (!pageIds.containsKey(rid.pageNo.pid)) {
			return null; // not found
		}

		HFPage find = new HFPage();
		SystemDefs.JavabaseBM.pinPage(rid.pageNo, find, false);
		SystemDefs.JavabaseBM.unpinPage(rid.pageNo, false);

		RID curRid = find.firstRecord();
		while (curRid != null) {
			if (curRid.equals(rid)) {
				// found
				return find.getRecord(curRid);
			}
			curRid = find.nextRecord(curRid);
		}
		return null;
	}

	public Scan openScan() throws IOException, ReplacerException,
			HashOperationException, PageUnpinnedException,
			InvalidFrameNumberException, PageNotReadException,
			BufferPoolExceededException, PagePinnedException, BufMgrException {

		SystemDefs.JavabaseBM.pinPage(headerPageId,headerPage, false);
		return new Scan(this);
	}

	public void deleteFile() throws InvalidBufferException, ReplacerException,
			HashOperationException, InvalidFrameNumberException,
			PageNotReadException, BufferPoolExceededException,
			PagePinnedException, PageUnpinnedException,
			HashEntryNotFoundException, BufMgrException, DiskMgrException,
			IOException, FileEntryNotFoundException, FileIOException,
			InvalidPageNumberException {

		SystemDefs.JavabaseDB.delete_file_entry(fileName);
	}

	public int getRecordsNumber() {
		return records;
	}

}
