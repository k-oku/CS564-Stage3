#include <memory.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <fcntl.h>
#include <iostream>
#include <stdio.h>
#include "page.h"
#include "buf.h"

#define ASSERT(c)  { if (!(c)) { \
		       cerr << "At line " << __LINE__ << ":" << endl << "  "; \
                       cerr << "This condition should hold: " #c << endl; \
                       exit(1); \
		     } \
                   }

//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------

BufMgr::BufMgr(const int bufs)
{
    numBufs = bufs;

    bufTable = new BufDesc[bufs];
    memset(bufTable, 0, bufs * sizeof(BufDesc));
    for (int i = 0; i < bufs; i++) 
    {
        bufTable[i].frameNo = i;
        bufTable[i].valid = false;
    }

    bufPool = new Page[bufs];
    memset(bufPool, 0, bufs * sizeof(Page));

    int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
    hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

    clockHand = bufs - 1;
}


BufMgr::~BufMgr() {

    // flush out all unwritten pages
    for (int i = 0; i < numBufs; i++) 
    {
        BufDesc* tmpbuf = &bufTable[i];
        if (tmpbuf->valid == true && tmpbuf->dirty == true) {

#ifdef DEBUGBUF
            cout << "flushing page " << tmpbuf->pageNo
                 << " from frame " << i << endl;
#endif

            tmpbuf->file->writePage(tmpbuf->pageNo, &(bufPool[i]));
        }
    }

    delete [] bufTable;
    delete [] bufPool;
}


const Status BufMgr::allocBuf(int & frame) 
{
    int pinned = 0;
    unsigned int initPos = clockHand;
    BufDesc *currBuf;

    while (true) {
        if (initPos == clockHand && pinned >= numBufs) {
            return BUFFEREXCEEDED;
        } else {
            //pinned = 0;
        }
            
        advanceClock();
        currBuf = &bufTable[clockHand];
        
        if (currBuf->valid) {
            if (currBuf->refbit) {
                currBuf->refbit = false;
            } else {
                if (currBuf->pinCnt == 0) {
                    if (currBuf->dirty) {
                        if (currBuf->file->writePage(currBuf->pageNo, &(bufPool[clockHand])) != OK) {
                            return UNIXERR;
                        }
                        currBuf->dirty = false;
                        hashTable->remove(currBuf->file, currBuf->pageNo);
                        //disposePage(currBuf->file, currBuf->pageNo);

                    } else {
                        hashTable->remove(currBuf->file, currBuf->pageNo);
                        //disposePage(currBuf->file, currBuf->pageNo);
                        break;
                    }
                        
                } else 
                    ++pinned;
            }
        } else {
            break;
        }
            
    }
    currBuf->Clear();
    frame = currBuf->frameNo;
    return OK;
}

	
/*
readPage reads a page that is either in the buffer pool or in the file.

@params:
File* file: file ptr 
const int PageNo: page # to be read
Page*& page: pointer to page within the frame

Returns:
Status: OK, UNIXERR, BUFFEREXCEEDED, or HASHTBLERROR
OK if no errors occurred
UNIXERR if a unix error occurred 
BUFFEREXCEEDED if all buffer frames are pinned
HASHTBLERROR if a hash table error occurred 
*/
	
const Status BufMgr::readPage(File* file, const int PageNo, Page*& page)
{   
    // Brandon's method
    int frame = 0;
    // first, check whether page is in buffer pool.

    Status statusCheck = OK; // to check status, similar usage to other funcs
    statusCheck = hashTable->lookup(file, PageNo, frame); // should set "frame" int

    if (statusCheck == HASHTBLERROR) { // make sure lookup works
        return HASHTBLERROR;
    } 
    else if (statusCheck == HASHNOTFOUND) { // check if page is in buffer pool
        // page not in buffer pool, case 1
        // call allocBuf to alloc buffer frame
        statusCheck = allocBuf(frame);
        if (statusCheck != OK) {
            return statusCheck; // ought to be UNIXERR or BUFFEREXCEEDED
        }
        // buffer frame alloc'd
        // now call file->readPage() 
        statusCheck = file->readPage(PageNo, &bufPool[frame]);
        if (statusCheck != OK) {
	    disposePage(file, PageNo); // get rid of page read
            return statusCheck; // something wrong with readpage 
        }
        // insert page into hashtable
        statusCheck = hashTable->insert(file, PageNo, frame);

        if (statusCheck != OK) {
            return statusCheck; // ought to be HASHTBLERROR
        }
        // invoke Set() on frame to set it up properly, leaving pinCnt for page set to 1.
        BufDesc& currBufTable = bufTable[frame];
        currBufTable.Set(file, PageNo);
        currBufTable.frameNo = frame; // need to update this as frame changed
        bufTable[frame] = currBufTable;
        // return a pointer to the frame containing the page via the page parameter
        
        page = &bufPool[frame];

    } else {
        // page is in buffer pool, case 2
        // set appropriate refbit
        bufTable[frame].refbit = true; // ref'd recently (read), so "true"
        // increment pinCnt for page
        (bufTable[frame].pinCnt)++;
        // return pointer to frame containing page via page parameter
        page = &bufPool[frame];
    }
    return OK;

}


const Status BufMgr::unPinPage(File* file, const int PageNo, 
			       const bool dirty) 
{
    int frame = 0;
    BufDesc *buf;
    
    if (hashTable->lookup(file, PageNo, frame) != OK)
        return HASHNOTFOUND;
    
    buf = &bufTable[frame];
    
    if (buf->pinCnt == 0)
        return PAGENOTPINNED;
    
    --(buf->pinCnt);

    if (dirty) { // if dirty, then set dirty bit
        buf->dirty = true;
    }
    
    return OK;
}

const Status BufMgr::allocPage(File* file, int& pageNo, Page*& page) 
{
    
    Status status;

    //allocate empty page in file
    status = file->allocatePage(pageNo);
    //pageNo is now the page number of newly allocated page
    int pageNum = pageNo;

    if (status == UNIXERR) {
        return UNIXERR;
    }
    else {
        //obtain buffer pool frame
        status = allocBuf(pageNo);
        //pageNo is now frameNo of buffer pool frame

        //address using buf table
        if(status == BUFFEREXCEEDED) {
            return BUFFEREXCEEDED;
        } else if (status == UNIXERR) {
            return UNIXERR;
        }
        else {
            //insert entry into hashtable
            status = hashTable->insert(file, pageNum, pageNo);
            if(status == HASHTBLERROR) {
                return HASHTBLERROR;
            }
            
            //invoke Set() on frame
            BufDesc& currBuf = bufTable[pageNo];
            currBuf.Set(file, pageNum);
   
            //return pointer to the buffer frame allocated for the page
            page = &bufPool[pageNo];

            //return page number of newly allocated page via pageNo
            pageNo = pageNum;
           
            status = OK;
        }
    }
    return status;
}

const Status BufMgr::disposePage(File* file, const int pageNo) 
{
    // see if it is in the buffer pool
    Status status = OK;
    int frameNo = 0;
    status = hashTable->lookup(file, pageNo, frameNo);
    if (status == OK)
    {
        // clear the page
        bufTable[frameNo].Clear();
    }
    status = hashTable->remove(file, pageNo);

    // deallocate it in the file
    return file->disposePage(pageNo);
}

const Status BufMgr::flushFile(const File* file) 
{
  Status status;

  for (int i = 0; i < numBufs; i++) {
    BufDesc* tmpbuf = &(bufTable[i]);
    if (tmpbuf->valid == true && tmpbuf->file == file) {

      if (tmpbuf->pinCnt > 0)
	  return PAGEPINNED;

      if (tmpbuf->dirty == true) {
#ifdef DEBUGBUF
	cout << "flushing page " << tmpbuf->pageNo
             << " from frame " << i << endl;
#endif
	if ((status = tmpbuf->file->writePage(tmpbuf->pageNo,
					      &(bufPool[i]))) != OK)
	  return status;

	tmpbuf->dirty = false;
      }

      hashTable->remove(file,tmpbuf->pageNo);

      tmpbuf->file = NULL;
      tmpbuf->pageNo = -1;
      tmpbuf->valid = false;
    }

    else if (tmpbuf->valid == false && tmpbuf->file == file)
      return BADBUFFER;
  }
  
  return OK;
}


void BufMgr::printSelf(void) 
{
    BufDesc* tmpbuf;
  
    cout << endl << "Print buffer...\n";
    for (int i=0; i<numBufs; i++) {
        tmpbuf = &(bufTable[i]);
        cout << i << "\t" << (char*)(&bufPool[i]) 
             << "\tpinCnt: " << tmpbuf->pinCnt;
    
        if (tmpbuf->valid == true)
            cout << "\tvalid\n";
        cout << endl;
    };
}


