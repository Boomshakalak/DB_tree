/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include "btree.h"
#include "filescan.h"
#include "exceptions/bad_index_info_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/end_of_file_exception.h"

//#define DEBUG

namespace badgerdb
{

// -----------------------------------------------------------------------------
// BTreeIndex::BTreeIndex -- Constructor
// -----------------------------------------------------------------------------

BTreeIndex::BTreeIndex(const std::string & relationName,
		std::string & outIndexName,
		BufMgr *bufMgrIn,
		const int attrByteOffset,
		const Datatype attrType)
{
	std::ostringstream idxStr;
	idxStr<<relationName<<'.'<< attrByteOffset;
	std::string indexName = idxStr.str();
	outIndexName = indexName;
	bufMgr = bufMgrIn;
	this->attrByteOffset = attrByteOffset;
	this->attributeType = attrType;
	if ( File::exists(indexName) ) {
		file = new BlobFile(outIndexName,false);
		Page* metaPage;
		bufMgr->readPage(file,file->getFirstPageNo(),metaPage);
		bufMgr->unPinPage(file,file->getFirstPageNo(),false);
		IndexMetaInfo* meta = reinterpret_cast<IndexMetaInfo*>(metaPage);
		this->rootPageNum = meta->rootPageNo;
	}
	else 
		{
			file = new BlobFile(indexName,true);
			Page *metaPage,*rootPage, *firstLeaf;
			PageId pid;
			bufMgr->allocPage(file, headerPageNum, metaPage);
			bufMgr->allocPage(file, rootPageNum, rootPage);
			bufMgr->allocPage(file, pid, firstLeaf);
			NonLeafNodeInt* root = reinterpret_cast<NonLeafNodeInt*>(rootPage);
			*root = NonLeafNodeInt();
			root->k=1;
			root->level = 1;
			root->pageNoArray[0] = pid;
			LeafNodeInt* fLeaf;
			fLeaf = reinterpret_cast<LeafNodeInt*>(firstLeaf);
			*fLeaf = LeafNodeInt();
			bufMgr->unPinPage(file,rootPageNum,true);
			bufMgr->unPinPage(file,pid,true);
			FileScan fs(relationName,bufMgr);
			try
			{
				RecordId scanRid;
				while(1)
				{
					fs.scanNext(scanRid);
					std::string recordStr = fs.getRecord();
					const char *record = recordStr.c_str();
					void* key = (void*)(record + attrByteOffset);
					insertEntry(key,scanRid);
				}
			}
			catch(EndOfFileException e)
			{
			}
			IndexMetaInfo* meta = reinterpret_cast<IndexMetaInfo*>(metaPage);
			// meta->relationName = relationName;
			strcpy(meta->relationName,relationName.c_str());
			meta->attrByteOffset = attrByteOffset;
			meta->attrType = attrType;
			meta->rootPageNo = rootPageNum;
			bufMgr->unPinPage(file,headerPageNum,true);
		}
	
}


// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

BTreeIndex::~BTreeIndex()
{
	bufMgr->flushFile(file);
	delete file;
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

const void BTreeIndex::insertEntry(const void *key, const RecordId rid) 
{
	int val = *((int*)(key));
	RIDKeyPair<int> element ;
	element.set(rid, val);
	Page* rpg;
	bufMgr->readPage(file,rootPageNum,rpg);
	PageId temp;
	NonLeafNodeInt* root = reinterpret_cast<NonLeafNodeInt*>(rpg);
	if (root->k ==INTARRAYNONLEAFSIZE){
		Page* newRoot;
		temp = rootPageNum;
		bufMgr->allocPage(file,rootPageNum,newRoot);
		NonLeafNodeInt* nRoot = reinterpret_cast<NonLeafNodeInt*>(newRoot);
		*nRoot=NonLeafNodeInt();
		nRoot->pageNoArray[0] = temp;
		splitChildren(nRoot,0);
		bufMgr->unPinPage(file,temp,true);
		insertNonFull(nRoot,val,rid);
	}
	else
		insertNonFull(root,val,rid);
	bufMgr->unPinPage(file,rootPageNum,true);
}

// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------

const void BTreeIndex::startScan(const void* lowValParm,
				   const Operator lowOpParm,
				   const void* highValParm,
				   const Operator highOpParm)
{

	scanExecuting = true;
	if (lowOpParm != GT && lowOpParm !=GTE ){
		throw BadOpcodesException();
	}
	if(highOpParm != LT && highOpParm != LTE){
		throw BadOpcodesException();
	}

	this->lowOp = lowOpParm;
	this->highOp = highOpParm;

	// Call tmplate helper to do the work.
	if(attributeType == INTEGER) {
		this->lowValInt = *((int*) lowValParm);
		this->highValInt = *((int *) highValParm);
		startScanHelper<int, NonLeafNodeInt>(*((int*) lowValParm), *((int *) highValParm), INTARRAYNONLEAFSIZE);
	} else if (attributeType == DOUBLE) {
		this->lowValDouble = *((double*) lowValParm);
		this->highValDouble = *((double *) highValParm);
		startScanHelper<double , NonLeafNodeDouble>(*((double *) lowValParm), *((double *) highValParm), DOUBLEARRAYNONLEAFSIZE);
	} else {
		strncpy((char*) this->lowValString.c_str(), (char *)lowValParm, STRINGSIZE-1);
		strncpy(lowValChar, (char *)lowValParm, STRINGSIZE);
		strncpy((char*) this->highValString.c_str(), (char *)highValParm, STRINGSIZE-1);
		strncpy(highValChar, (char *)highValParm, STRINGSIZE);
		startScanHelper<char[STRINGSIZE], NonLeafNodeString> (lowValChar, highValChar, STRINGARRAYNONLEAFSIZE);
	}	

}
/*
 * Helper method of startScan, basically it base on the low bound to find the first position of the entry.
 * This function helps the startScan function.
 * T is the data type.
 * T1 is the non-leaf struct.
 * This function is called by the startScan and do the work.
 * */
template<class T, class T1>
void BTreeIndex::startScanHelper(T lowValParm,
								 T highValParm,
								 int NONLEAFARRAYMAX)
{
	if(biggerThan<T> (lowValParm,  highValParm))
		throw BadScanrangeException();

	//find first one
	currentPageNum = rootPageNum;
	bufMgr->readPage(file, currentPageNum, currentPageData);
	T1* nonLeafNode = (T1*) currentPageData;

	int pos = 0;
	while(nonLeafNode->level != 1) {
		// If current level is not 1, then next page is not leaf page.
		// Still need to go to next level.
		pos = 0;
		while(!smallerThan<T> (lowValParm, nonLeafNode->keyArray[pos])
			  && nonLeafNode->pageNoArray[pos + 1] != 0
			  && pos < NONLEAFARRAYMAX)
			pos++;
		PageId nextPageId = nonLeafNode->pageNoArray[pos];
		bufMgr->readPage(file, nextPageId, currentPageData);
		bufMgr->unPinPage(file, currentPageNum, false);
		currentPageNum = nextPageId;
		nonLeafNode = (T1*) currentPageData;
	}

	// This page is level 1, which means next page is leaf node.
	pos = 0;
	while(!smallerThan<T> (lowValParm, nonLeafNode->keyArray[pos])
		  && nonLeafNode->pageNoArray[pos + 1] != 0
		  && pos < NONLEAFARRAYMAX)
		pos++;
	PageId nextPageId = nonLeafNode->pageNoArray[pos];
	bufMgr->readPage(file, nextPageId, currentPageData);
	bufMgr->unPinPage(file, currentPageNum, false);
	currentPageNum = nextPageId;
	nextEntry = 0;
}
// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

const void BTreeIndex::scanNext(RecordId& outRid) 
{
	if(scanExecuting == false)
		throw ScanNotInitializedException();

	if(attributeType == INTEGER)
		scanNextHelper<int, LeafNodeInt> (outRid, lowValInt, highValInt, INTARRAYLEAFSIZE);
	else if (attributeType == DOUBLE)
		scanNextHelper<double, LeafNodeDouble> (outRid, lowValDouble, highValDouble, DOUBLEARRAYLEAFSIZE);
	else
		scanNextHelper<char[STRINGSIZE], LeafNodeString> (outRid, lowValChar, highValChar, STRINGARRAYLEAFSIZE);
}
template <class T, class T1>
void BTreeIndex::scanNextHelper(RecordId &outRid, T lowVal, T highVal, int ARRAYMAX)
{
	T1* leafNode;
	while(1){
		leafNode = (T1*) currentPageData;

		// Go to next page.
		if(leafNode->ridArray[nextEntry].page_number == 0 || nextEntry == ARRAYMAX) {
			PageId nextPageNum = leafNode->rightSibPageNo;
			if(nextPageNum == 0){
				// Next page is 0, scan finish.
				bufMgr->unPinPage(file, currentPageNum, false);
				throw IndexScanCompletedException();
			}

			bufMgr->unPinPage(file, currentPageNum, false);
			currentPageNum = nextPageNum;

			bufMgr->readPage(file, currentPageNum, currentPageData);
			nextEntry = 0;
			continue;
		}

		// Do not satisfy.
		if((lowOp==GT && !biggerThan<T> (leafNode->keyArray[nextEntry], lowVal) )
		   || (lowOp==GTE && smallerThan<T> (leafNode->keyArray[nextEntry], lowVal))
		   ) {
			nextEntry++;
			continue;
		}

		// Value bigger that high value, scan end.
		if((highOp==LT && !smallerThan<T> (leafNode->keyArray[nextEntry],  highVal))
		   || (highOp==LTE && biggerThan<T> (leafNode->keyArray[nextEntry], highVal) ))
			throw IndexScanCompletedException();

		// Got a record.
		outRid = leafNode->ridArray[nextEntry];
		nextEntry++;
		return ;
	}
}
// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
const void BTreeIndex::endScan() 
{
	if(scanExecuting == false)
		throw ScanNotInitializedException();
	scanExecuting = false;

	try {
		bufMgr->unPinPage(file, currentPageNum, false);
	} catch (PageNotPinnedException e) {
	} catch (HashNotFoundException e) {
	}
}
void BTreeIndex::splitChildren(NonLeafNodeInt* node, int c)
{
	Page* curr;
	Page* subl;
	PageId pN;
	int key;
	bufMgr->allocPage(file,pN,subl);
	bufMgr->readPage(file,node->pageNoArray[c],curr);
	if (node->level != 1 ){
		NonLeafNodeInt* nlNodeR = reinterpret_cast<NonLeafNodeInt*>(subl);
		NonLeafNodeInt* nlNodeL = reinterpret_cast<NonLeafNodeInt*>(curr);
		nlNodeR->level = nlNodeL->level;
		nlNodeR->k = INTARRAYNONLEAFSIZE/2;
		for (int i = 0; i < INTARRAYNONLEAFSIZE/2; ++i)
		{
			nlNodeR->keyArray[i] = nlNodeL->keyArray[i+(INTARRAYNONLEAFSIZE+1)/2];
		}
		for (int i = 0; i < INTARRAYNONLEAFSIZE/2+1; ++i)
		{
			nlNodeR->pageNoArray[i] = nlNodeL->pageNoArray[i+(INTARRAYNONLEAFSIZE+1)/2];
		}	
		nlNodeL->k = (INTARRAYNONLEAFSIZE-1)/2;
		key =  nlNodeL->keyArray[nlNodeL->k];
	}
	if (node->level == 1){
		LeafNodeInt* lNodeR = reinterpret_cast<LeafNodeInt*>(subl);
		LeafNodeInt* lNodeL = reinterpret_cast<LeafNodeInt*>(curr);
		lNodeR->k = INTARRAYLEAFSIZE/2;
		for (int i = 0; i < INTARRAYLEAFSIZE/2; ++i)
		{
			lNodeR->keyArray[i] = lNodeL->keyArray[i+(INTARRAYLEAFSIZE+1)/2];
		}
		for (int i = 0; i < INTARRAYLEAFSIZE/2; ++i)
		{
			lNodeR->ridArray[i] = lNodeL->ridArray[i+(INTARRAYLEAFSIZE+1)/2];
		}	
		lNodeL->k = (INTARRAYLEAFSIZE+1)/2;
		lNodeL->rightSibPageNo = pN;
		key =  lNodeL->keyArray[lNodeL->k-1];
	}
	for (int i = node->k+1; i>c+1; --i)
	{
		node->pageNoArray[i] = node->pageNoArray[i-1];
	}
	node->pageNoArray[c+1] = pN;
	for (int i = node->k; i > c; --i)
	{
		node->keyArray[i] = node->keyArray[i-1];
	}
	node->keyArray[c]= key;
	node->k++;
	bufMgr->unPinPage(file,pN,true);
	bufMgr->unPinPage(file,node->pageNoArray[c],true);
}
void BTreeIndex::insertNonFull(NonLeafNodeInt* node , int val, RecordId rid)
{
	Page* pg;
	int pos = node->k-1;
	if (node->level!=1){
		for (pos >= 0; val < node->keyArray[pos]; pos--){
		}
		pos++;
		bufMgr->readPage(file,node->pageNoArray[pos],pg);
		NonLeafNodeInt* child = reinterpret_cast<NonLeafNodeInt*>(pg);
		if (child->k == INTARRAYNONLEAFSIZE) {
			splitChildren(node,pos);
			if (val>node->keyArray[pos]) {
				pos++;
				bufMgr->readPage(file,node->pageNoArray[pos],pg);
				bufMgr->unPinPage(file,node->pageNoArray[pos-1],true);
			}
		}
		child = reinterpret_cast<NonLeafNodeInt*>(pg);
		insertNonFull(child, val,rid);
		bufMgr->unPinPage(file,node->pageNoArray[pos],true);
		return;
	}
	if (node->level ==1){
		for (pos >= 0; val < node->keyArray[pos]; pos--){
		}
		pos++;
		bufMgr->readPage(file,node->pageNoArray[pos],pg);
		LeafNodeInt* child = reinterpret_cast<LeafNodeInt*>(pg);
		if (child->k == INTARRAYLEAFSIZE){
			splitChildren(node,pos);
			if (val>node->keyArray[pos]) {
				pos++;
				bufMgr->readPage(file,node->pageNoArray[pos],pg);
				bufMgr->unPinPage(file,node->pageNoArray[pos-1],true);
			}
		}
		child = reinterpret_cast<LeafNodeInt*>(pg);
		int i;
		for (i = child->k-1; i >=0 && val<child->keyArray[i]; --i)
		{
			child->keyArray[i+1]=child->keyArray[i];
		}
		child->keyArray[i+1] = val;
		child->ridArray[i+1] = rid;
		child->k++;
		bufMgr->unPinPage(file,node->pageNoArray[pos],true);
		return;
	}
}

}
