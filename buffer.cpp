/**
   * @author See Contributors.txt for code contributors and overview of BadgerDB.
   *
   * @section LICENSE
   * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
   * 
   * Stefan Maurer 907 955 7557
   * Nicholas Moeller 907 519 6262
   * Nitin Challa 907 967 0536
   */
   
/** 
  *The central class which manages the buffer pool 
  *including frame allocation and deallocation to pages in the file.
  */

  #include <memory>
  #include <iostream>
  #include "buffer.h"
  #include "exceptions/buffer_exceeded_exception.h"
  #include "exceptions/page_not_pinned_exception.h"
  #include "exceptions/page_pinned_exception.h"
  #include "exceptions/bad_buffer_exception.h"
  #include "exceptions/hash_not_found_exception.h"

  namespace badgerdb { 

  //----------------------------------------
  // Constructor of the class BufMgr
  //----------------------------------------

  BufMgr::BufMgr(std::uint32_t bufs)
    : numBufs(bufs) {
    bufDescTable = new BufDesc[bufs];

    for (FrameId i = 0; i < bufs; i++) 
    {
      bufDescTable[i].frameNo = i;
      bufDescTable[i].valid = false;
    }

    bufPool = new Page[bufs];

    int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
    hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

    clockHand = bufs - 1;
  }

	/**
   * Destructor of BufMgr class
	 */
  BufMgr::~BufMgr() {
    delete[] bufDescTable;
    delete[] bufPool;
    delete hashTable;
  }

  void BufMgr::advanceClock()
  {   
    clockHand = (clockHand + 1) % numBufs;
  }

  void BufMgr::allocBuf(FrameId & frame) 
  {

    std::uint32_t count = 0;
    while (count <= numBufs) {
      advanceClock();
      if (bufDescTable[clockHand].valid) {
        if (bufDescTable[clockHand].refbit) {
          bufDescTable[clockHand].refbit = false;
          continue;
        }
        if (bufDescTable[clockHand].pinCnt > 0) {
            count++;
            continue;
        }
        if (bufDescTable[clockHand].dirty) {
          bufDescTable[clockHand].file->writePage(bufPool[clockHand]);
          hashTable->remove(bufDescTable[clockHand].file, bufDescTable[clockHand].pageNo);
          bufDescTable[clockHand].dirty = false;
          frame = clockHand;
          return;
        }
        //hashTable->remove(bufDescTable[clockHand].file, bufDescTable[clockHand].pageNo);
        frame = clockHand;
        return;     
      }
      frame = clockHand;
      return;
    }
    throw BufferExceededException(); 
  }

	/**
	 * Reads the given page from the file into a frame and returns the pointer to page.
	 * If the requested page is already present in the buffer pool pointer to that frame is returned
	 * otherwise a new frame is allocated from the buffer pool for reading the page.
	 *
	 * @param file   	File object
	 * @param PageNo  Page number in the file to be read
	 * @param page  	Reference to page pointer. Used to fetch the Page object in which requested page from file is read in.
	 */
  void BufMgr::readPage(File* file, const PageId pageNo, Page*& page)
  {
    FrameId frameNumber;
    try{
      hashTable->lookup(file, pageNo, frameNumber);
      bufDescTable[frameNumber].pinCnt++;
      bufDescTable[frameNumber].refbit = true;
      page = &bufPool[frameNumber];
    }
    catch(HashNotFoundException& e){
      allocBuf(frameNumber); //Call allocBuf() to allocate a buffer frame
      bufPool[frameNumber] = file->readPage(pageNo); //call the method file->readPage() to read the page from disk into the buffer pool frame
      hashTable->insert(file, pageNo, frameNumber); // Next, insert the page into the hashtable
      bufDescTable[frameNumber].Set(file, pageNo); //Finally, invoke Set() on the frame to set it up properly
      page = &bufPool[frameNumber]; //Return a pointer to the frame containing the page via the page parameter
    }

  }

	/**
	 * Unpin a page from memory since it is no longer required for it to remain in memory.
	 *
	 * @param file   	File object
	 * @param PageNo  Page number
	 * @param dirty		True if the page to be unpinned needs to be marked dirty	
   * @throws  PageNotPinnedException If the page is not already pinned
	 */
  void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty) 
  {
    FrameId f;
    try{
      hashTable->lookup(file, pageNo, f);
      if (bufDescTable[f].pinCnt == 0) {
        throw PageNotPinnedException(file->filename(), pageNo, f);
      }
      bufDescTable[f].pinCnt = bufDescTable[f].pinCnt - 1;
      
      if(dirty)
        bufDescTable[f].dirty = true;
    }
    catch (HashNotFoundException& e) {}

  }

	/**
	 * Allocates a new, empty page in the file and returns the Page object.
	 * The newly allocated page is also assigned a frame in the buffer pool.
	 *
	 * @param file   	File object
	 * @param PageNo  Page number. The number assigned to the page in the file is returned via this reference.
	 * @param page  	Reference to page pointer. The newly allocated in-memory Page object is returned via this reference.
	 */
  void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page) 
  {
      //The first step in this method is to to allocate an empty page in the specified file by invoking the file->allocatePage() method.
      //This method will return a newly allocated page.
      
      //Then allocBuf() is called to obtain a buffer pool frame. 
      
      //Next, an entry is inserted into the hashtable 
      
      //and Set() is invoked on the frame to set it up properly. The method returns
      //both the page number of the newly allocated page to the caller via the pageNo parameter
      //and a pointer to the buffer frame allocated for the page via the page parameter.
      
      /*file->allocatePage();
      allocBuf(clockHand);
      hashTable->insert(file, pageNo, clockHand);
      bufDescTable[clockHand].Set(file, pageNo);
      */
      FrameId f;
      Page p = file->allocatePage();
      allocBuf(f);
      bufPool[f] = p;
      
      hashTable->insert(file, p.page_number(), f);
      bufDescTable[f].Set(file, p.page_number());
      page = &bufPool[f];
      pageNo = page->page_number();
      
  }

	/**
	 * Writes out all dirty pages of the file to disk.
	 * All the frames assigned to the file need to be unpinned from buffer pool before this function can be successfully called.
	 * Otherwise Error returned.
	 *
	 * @param file   	File object
   * @throws  PagePinnedException If any page of the file is pinned in the buffer pool 
   * @throws BadBufferException If any frame allocated to the file is found to be invalid
	 */
  void BufMgr::flushFile(const File* file) 
  {
    for (size_t i = 0; i < numBufs; i++)
    { 
      BufDesc frame = bufDescTable[i];
      if (!frame.valid)
        throw BadBufferException(frame.frameNo, frame.dirty, frame.valid, frame.refbit);
      
      if (frame.file == file)
      {
          if(frame.pinCnt != 0)
            throw PagePinnedException(frame.file->filename(), frame.pageNo, frame.frameNo);
          if(frame.dirty) {        
          frame.file->writePage(bufPool[i]);
          frame.dirty = false;
        }      
        hashTable->remove(frame.file, frame.pageNo);
        frame.Clear();
      }
    }
  }
  
	/**
	 * Delete page from file and also from buffer pool if present.
	 * Since the page is entirely deleted from file, its unnecessary to see if the page is dirty.
	 *
	 * @param file   	File object
	 * @param PageNo  Page number
	 */
  void BufMgr::disposePage(File* file, const PageId PageNo)
  {
    FrameId f;
    try {
    hashTable->lookup(file, PageNo, f);
    hashTable->remove(file, PageNo);
    bufDescTable[f].Clear();
    file->deletePage(PageNo);
    } catch (...){/*do nothing*/}
  }
  
	/**
   * Print member variable values. 
	 */
  void BufMgr::printSelf(void) 
  {
    BufDesc* tmpbuf;
    int validFrames = 0;
    
    for (std::uint32_t i = 0; i < numBufs; i++)
    {
      tmpbuf = &(bufDescTable[i]);
      std::cout << "FrameNo:" << i << " ";
      tmpbuf->Print();

      if (tmpbuf->valid == true)
        validFrames++;
    }

    std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
  }
}
