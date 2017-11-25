
#ifndef _rm_h_
#define _rm_h_

#include <string>
#include <vector>
#include <unordered_map>
#include "../rbf/rbfm.h"
#include "../ix/ix.h"

using namespace std;

# define RM_EOF (-1)  // end of a scan operator

// RM_ScanIterator is an iteratr to go through tuples
class RM_ScanIterator {
public:
  RM_ScanIterator() {};
  ~RM_ScanIterator() {};
  RBFM_ScanIterator rbfm_ScanIterator;
  // "data" follows the same format as RelationManager::insertTuple()
  RC getNextTuple(RID &rid, void *data) { 
    return rbfm_ScanIterator.getNextRecord(rid,data); };
  RC close() { return rbfm_ScanIterator.close(); };
};

// RM_IndexScanIterator is an iterator to go through index entries
class RM_IndexScanIterator {
public:
	RM_IndexScanIterator() {};  	// Constructor
	~RM_IndexScanIterator() {}; 	// Destructor

									// "key" follows the same format as in IndexManager::insertEntry()
	RC getNextEntry(RID &rid, void *key) { return ixScanIterator.getNextEntry(rid, data); };  	// Get next matching entry
	RC close() { return ixScanIterator.close(); };             			// Terminate index scan

public:
	IX_ScanIterator ixScanIterator;
};

// Relation Manager
class RelationManager
{
public:
  RecordBasedFileManager *fm_table;
  FileHandle fh_table;
  FileHandle fh_col;
  unordered_map<string, vector<Attribute>> map_attributes;
  unordered_map<string, vector<vector<Attribute>>> map_versionTable;
  unordered_map<string, bool> map_hasIndex;

  static RelationManager* instance();

  RC createCatalog();

  RC deleteCatalog();

  RC createTable(const string &tableName, const vector<Attribute> &attrs);

  RC deleteTable(const string &tableName);

  RC getAttributes(const string &tableName, vector<Attribute> &attrs);

  RC insertTuple(const string &tableName, const void *data, RID &rid);

  RC deleteTuple(const string &tableName, const RID &rid);

  RC updateTuple(const string &tableName, const void *data, const RID &rid);

  RC readTuple(const string &tableName, const RID &rid, void *data);

  // Print a tuple that is passed to this utility method.
  // The format is the same as printRecord().
  RC printTuple(const vector<Attribute> &attrs, const void *data);

  RC readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data);
  void prepareTuple_tables(int attributeCount, unsigned char *nullAttributesIndicator, const int Table_ID, const int lengthOftable, const string &tableName, const int lengthOffie,const string &fieldName, const int isSys, void *buffer, int *tupleSize);
  void prepareTuple_cols(int attributeCount, unsigned char *nullAttributesIndicator, const int Table_ID, const int lengthOffield, const string &fieldName, const int fieldOfType, const int max_length, const int field_ID, const int startVersion, const int endVersion, const int hasIndex, void *buffer, int *tupleSize);
  // Scan returns an iterator to allow the caller to go through the results one by one.
  // Do not store entire results in the scan iterator.
  RC scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,                  // comparison type such as "<" and "="
      const void *value,                    // used in the comparison
      const vector<string> &attributeNames, // a list of projected attributes
      RM_ScanIterator &rm_ScanIterator);

  string getIndexName(const string &tableName, const string &attributeName);

  int getTableID(const string &tableName);

  bool hasIndex(const string &tableName, const string &attributeName);

  RC createIndex(const string &tableName, const string &attributeName);

  RC destroyIndex(const string &tableName, const string &attributeName);

  // indexScan returns an iterator to allow the caller to go through qualified entries in index
  RC indexScan(const string &tableName,
	  const string &attributeName,
	  const void *lowKey,
	  const void *highKey,
	  bool lowKeyInclusive,
	  bool highKeyInclusive,
	  RM_IndexScanIterator &rm_IndexScanIterator);

// Extra credit work (10 points)
public:
  RC addAttribute(const string &tableName, const Attribute &attr);

  RC dropAttribute(const string &tableName, const string &attributeName);

  vector<vector<Attribute>> generateVersionTable(const string &tableName);

  vector<vector<Attribute>> versionTable;
  vector<Attribute>* getVersionTable(const int version);

protected:
  RelationManager();
  ~RelationManager();

};

#endif
