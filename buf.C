/*
* Brandon Frauenfeld, 907 708 3880, frauenfeld
* Kath Oku, 907 869 6235, kmoku
* Khai Bui, 907 264 9824, kmbui2
*
* This file is a part of the Buffer Manager for the database management system - Minirel -  
* specified in Project Stage 3 description
* 
* buf.C implements the BufMgr class as declared in buf.h
*/

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

/*
* Allocate a buffer frame using the LRU approximation - clock - algorithm
*
* @param frame Reference to the allocated frame's number
*
* @return OK if successful, or BUFFEREXCEEDED if buffer limit was exceeded, or UNIXERR if encounterd an error in the I/O layer  
*/
const Status BufMgr::allocBuf(int & frame) 
{
    int pinned = 0;
    unsigned int initPos = clockHand;
    BufDesc *currBuf;

    while (true) {
        // Check buffer limit every rotation
        if (initPos == clockHand) {
            if (pinned >= numBufs) {
                return BUFFEREXCEEDED;
            } else {
                pinned = 0;
            }        
        } 

        // Move to the next buffer frame
        advanceClock();
        currBuf = &bufTable[clockHand];
        
        // Allocate frame
        // Check if this frame is valid
        if (currBuf->valid) {
            // Check if this frame was recently referenced
            if (currBuf->refbit) {
                currBuf->refbit = false;
            } else {
                // Check if this frame is pinned
                if (currBuf->pinCnt == 0) {
                    // Lazily write page if this frame is dirty
                    if (currBuf->dirty) {
                        if (currBuf->file->writePage(currBuf->pageNo, &(bufPool[clockHand])) != OK) {
                            return UNIXERR;
                        }
                    }
                    // Remove frame's content from buffer
                    hashTable->remove(currBuf->file, currBuf->pageNo);
                    break;
                } else {
                    ++pinned;
                }
            }
        } else {
            break;
        }
    }

    // Assign the allocated frame number
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

/*
* Unpin a page's corresponding buffer frame and set its' dirty bit
* 
* @param file Pointer to the file whose page will be unpinned 
* @param PageNo Number of the page that will be unpinned
* @param dirty Indicate whether the frame was modified
* 
* @return OK if successful, or HASHNOTFOUND if the page isn't in the buffer, or PAGENOTPINNED if the frame is already unpinned
*/
const Status BufMgr::unPinPage(File* file, const int PageNo, 
			       const bool dirty) 
{
    int frame;
    BufDesc *buf;

    // Find the page's buffer frame
    if (hashTable->lookup(file, PageNo, frame) != OK)
        return HASHNOTFOUND;
    
    buf = &bufTable[frame];
    
    if (buf->pinCnt == 0)
        return PAGENOTPINNED;
    
    // Unpin buffer frame
    --(buf->pinCnt);

    // If dirty, set dirty bit
    if (dirty) { 
        buf->dirty = true;
    }
    
    return OK;
}

/*
* Allocate a page to corresponding buffer frame
* @params file - pointer to file with page to be allocated
* @params pageNo - reference to page number of file to be allocated
* @params page - pointer by reference to page within frame
*
* @return OK if no errors occurred, UNIXERR if a Unix error occurred, BUFFEREXCEEDED if all buffer frames are pinned,
* or HASHTBLEERROR if a hash table error occurred
*
* returns the page number of the newly allocated page to the caller via the pageNo parameter 
* and a pointer to the buffer frame allocated for the page via the page parameter
*/
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


