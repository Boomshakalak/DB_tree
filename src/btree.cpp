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
	if ( File::exists(indexName) ) file = &BlobFile::open(indexName);
	else 
		{
			file = &BlobFile::create(indexName);
			file->allocatePage(headerPageNum);
			file->allocatePage(rootPageNum);
			IndexMetaInfo meta;
			meta.relationName = relationName;
			meta.attrByteOffset = attrByteOffset;
			meta.attrType = attrType;
			meta.rootPageNum =rootPageNum;
			Page metapage = *reinterpret_cast<Page*>(&meta);
			BlobFile::writePage(headerPageNum, metapage);
		}
	FileScan fs(relationName,bufMgrIn);
	try
	{
		RecordId scanRid;
		while(1)
		{
			fs.scanNext(scanRid);
			std::string recordStr = fs.getRecord();
			const char *record = recordStr.c_str();
			void *key = record + attrByteOffset;
			insertEntry(key,rid);
		}
	}
	catch(EndOfFileException e)
	{

	}
}


// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

BTreeIndex::~BTreeIndex()
{
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

const void BTreeIndex::insertEntry(const void *key, const RecordId rid) 
{
	int val = *(static_cast<int*>key);
	RIDKeyPair<int> element ;
	element.set(rid, val);
}

// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------

const void BTreeIndex::startScan(const void* lowValParm,
				   const Operator lowOpParm,
				   const void* highValParm,
				   const Operator highOpParm)
{

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

}
