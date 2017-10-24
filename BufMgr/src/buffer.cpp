/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
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


BufMgr::~BufMgr() {
  //flush pages that are dirty
  for(FrameId i = 0; i < sizeof(bufDescTable); i++) {
    if(bufDescTable[i].dirty==true){
      flushFile(bufDescTable[i].file);
    }
  }
  //dealocate bufDesc Array
  delete[] bufDescTable;
  //dealocate bufPool Array
  delete[] bufPool;
  //call this dealocation method since bufHashTbl class has none static variables
  delete hashTable;
}

void BufMgr::advanceClock()
{
  clockHand++;
  //migth be numbufs instead of sizeof(bufPool)
  clockHand = clockHand % numBufs;
  // FrameId curr = clockHand;
  // if(curr++ >= sizeof(bufPool)){
  //   clockHand = 0;
  // } else {
  //   clockHand++;
  // }
}

void BufMgr::allocBuf(FrameId & frame) 
{

}

	
/*
* First check whether the page is already in the buffer pool by invoking the lookup() method,
  which may throw HashNotFoundException when page is not in the buffer pool, on the
  hashtable to get a frame number. There are two cases to be handled depending on the
  outcome of the lookup() call:
    • Case 1: Page is not in the buffer pool. Call allocBuf() to allocate a buffer frame and
      then call the method file->readPage() to read the page from disk into the buffer pool
      frame. Next, insert the page into the hashtable. Finally, invoke Set() on the frame to
      set it up properly. Set() will leave the pinCnt for the page set to 1. Return a pointer
      to the frame containing the page via the page parameter.
    • Case 2: Page is in the buffer pool. In this case set the appropriate refbit, increment
      the pinCnt for the page, and then return a pointer to the frame containing the page
      via the page parameter

* EDGE CASE if buf is not found in BufDesc table for some reason
* ALSO does this have no return?????
*/

void BufMgr::readPage(File* file, const PageId pageNo, Page*& page)
{
  FrameId frame;
  // find FrameId

  try{
        //check whether page in buffer pool
        hashTable->lookup(file, pageNo, frame);
        //if found update refbit
        bufDescTable[frame].refbit = true;
        bufDescTable[frame].pinCnt++;
        page = &bufPool[frame];
        return;


    }
  catch(HashNotFoundException e){
        allocBuf(frame);
        file->readPage(pageNo);
        hashTable->insert(file, pageNo, frame);
        page = &bufPool[frame];
        return;

  }
}


void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty) 
{
    FrameId frame;
    try
    {
        hashTable->lookup(file, pageNo, frame);
        if(bufDescTable[frame].pinCnt == 0){
            throw PageNotPinnedException("",bufDescTable[frame].pageNo,bufDescTable[frame].frameNo);
        }
        else{
            bufDescTable[frame].pinCnt--;
            if(bufDescTable[frame].dirty == true){
                bufDescTable[frame].dirty = false;
            }
        }
    }
    catch(HashNotFoundException e)
    {
        return;
    }
}

void BufMgr::flushFile(const File* file) 
{
  for(FrameId i=0; i<numBufs; i++) {
    if(bufDescTable[i].file == file) {
      if(bufDescTable[i].valid) {
        if(bufDescTable[i].pinCnt==0){
          if(bufDescTable[i].dirty){
              //wait for piazza
              //file->writePage(bufPool[i]);
              bufDescTable[i].valid = false;
          }
        } else {
            throw PagePinnedException("",bufDescTable[i].pageNo, 
              bufDescTable[i].frameNo);
        } 
        hashTable->remove(file, bufDescTable[i].pageNo);
        bufDescTable->Clear();
        //bufDescTable[i] = *(new BufDesc());
      } else {
        throw BadBufferException(bufDescTable[i].frameNo,
         bufDescTable[i].dirty, bufDescTable[i].valid, bufDescTable[i].refbit);
      }
    }
  }
}

void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page) 
{
}

void BufMgr::disposePage(File* file, const PageId PageNo)
{
    
}

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
