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
		file = &BlobFile::open(indexName);
		Page* metaPage = &(file->readPage(file->getFirstPageNo()));
		IndexMetaInfo* meta = reinterpret_cast<IndexMetaInfo*>(metaPage);
		this->rootPageNum = meta->rootPageNo;
	}
	else 
		{
			file = &BlobFile::create(indexName);
			Page* metaPage, rootPage;
			bufMgr->allocatePage(file, headerPageNum, metaPage);
			bufMgr->allocatePage(file, rootPageNum, rootPage);
			NonLeafNodeInt* root = reinterpret_cast<NonLeafNodeInt*>(rootPage);
			*root = NonLeafNodeInt();
			root->level = 1;
			FileScan fs(relationName,bufMgr);
			try
			{
				RecordId scanRid;
				while(1)
				{
					fs.scanNext(scanRid);
					std::string recordStr = fs.getRecord();
					const char *record = recordStr.c_str();
					void *key = record + attrByteOffset;
					insertEntry(key,scanRid);
				}
			}
			catch(EndOfFileException e)
			{
			}
			IndexMetaInfo* meta = reinterpret_cast<IndexMetaInfo*>(metaPage);
			meta->relationName = relationName;
			meta->attrByteOffset = attrByteOffset;
			meta->attrType = attrType;
			meta->rootPageNo = rootPageNum;
			bufMgr->unPinPage(file,headerPageNum,true);
			bufMgr->unPinPage(file,rootPageNum,true);
		}
	
}


// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

BTreeIndex::~BTreeIndex()
{
	bufMgr->flushFile(file);
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

const void BTreeIndex::insertEntry(const void *key, const RecordId rid) 
{
	int val = *(static_cast<int*>(key));
	RIDKeyPair<int> element ;
	element.set(rid, val);
	Page* rpg;
	bufMgr->readPage(file,rootPageNum,pg);
	PageId temp;
	NonLeafNodeInt* root = reinterpret_cast<NonLeafNodeInt*>(rpg);
	if (root->k ==INTARRAYNONLEAFSIZE){
		Page* newRoot;
		temp = rootPageNum;
		bufMgr->allocatePage(file,rootPageNum,newRoot);
		NonLeafNodeInt* nRoot = reinterpret_cast<NonLeafNodeInt*>newRoot;
		*nRoot=NonLeafNodeInt();
		nRoot->pageNoArray[0] = temp;
		splitChildren(nRoot,0);
		bufMgr->unPinPage(file,temp,true);
		insertNonFull(nRoot,val);
	}
	else
		insertNonFull(root,val);
}

// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------

const void BTreeIndex::startScan(const void* lowValParm,
				   const Operator lowOpParm,
				   const void* highValParm,
				   const Operator highOpParm)
{
	int low = *reinterpret_cast<int*>lowValParm;
	int high = *reinterpret_cast<int*>highValParm;
}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

const void BTreeIndex::scanNext(RecordId& outRid) 
{

}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
const void BTreeIndex::endScan() 
{

}
BTreeIndex::splitChildren(NonLeafNodeInt* node, int c)
{
	Page* curr;
	Page* subl;
	PageId pN;
	int key;
	bufMgr->allocatePage(file,pN,subl);
	bufMgr->readPage(file,node->pageNoArray[c],curr);
	if (node->level != 1 ){
		NonLeafNodeInt* nlNodeR = reinterpret_cast<NonLeafNodeInt*> subl;
		NonLeafNodeInt* nlNodeL = reinterpret_cast<NonLeafNodeInt*> curr;
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
		LeafNodeInt* lNodeR = reinterpret_cast<LeafNodeInt*> subl;
		LeafNodeInt* lNodeL = reinterpret_cast<LeafNodeInt*> curr;
		nlNodeR->level = nlNodeL->level;
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
		key =  lNodeL->keyArray[nlNodeL->k-1];
	}
	for (int i = node->k+1; i>c+1; --i)
	{
		node->pageNoArray[i] = node->pageNoArray[i-1];
	}
	node->pageNo[c+1] = pN;
	for (int i = node->k; i > c; --i)
	{
		node->keyArray[i] = node->keyArray[i-1];
	}
	node->keyArray[c]= key;
	node->k++;
	bufMgr->unPinPage(file,pN,true);
	bufMgr->unPinPage(file,node->pageNoArray[c],true);
}
BTreeIndex::insertNonFull(NonLeafNodeInt* node , int val, RecordID rid)
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
		insertNonFull(child, val);
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
		child->key[i+1] = val;
		child->ridArray[i+1] = rid;
		child->k++;
		bufMgr->unPinPage(file,node->pageNoArray[pos],true);
		return;
	}
}

}
