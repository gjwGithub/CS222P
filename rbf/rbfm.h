#ifndef _rbfm_h_
#define _rbfm_h_

#include <string>
#include <vector>
#include <climits>
#include <cmath> 

#include "../rbf/pfm.h"

using namespace std;

class IX_ScanIterator;

// Record ID
typedef struct
{
  unsigned pageNum;    // page number
  unsigned slotNum;    // slot number in the page
} RID;


// Attribute
typedef enum { TypeInt = 0, TypeReal, TypeVarChar } AttrType;

typedef unsigned AttrLength;

struct Attribute {
    string   name;     // attribute name
    AttrType type;     // attribute type
    AttrLength length; // attribute length
};

// Comparison Operator (NOT needed for part 1 of the project)
typedef enum { EQ_OP = 0, // no condition// = 
           LT_OP,      // <
           LE_OP,      // <=
           GT_OP,      // >
           GE_OP,      // >=
           NE_OP,      // !=
           NO_OP       // no condition
} CompOp;

enum UpdatedRecordMark { Origin = 0, Tombstone, UpdatedRecord};

/********************************************************************************
The scan iterator is NOT required to be implemented for the part 1 of the project 
********************************************************************************/

# define RBFM_EOF (-1)  // end of a scan operator

// RBFM_ScanIterator is an iterator to go through records
// The way to use it is like the following:
//  RBFM_ScanIterator rbfmScanIterator;
//  rbfm.open(..., rbfmScanIterator);
//  while (rbfmScanIterator(rid, data) != RBFM_EOF) {
//    process the data;
//  }
//  rbfmScanIterator.close();

class RBFM_ScanIterator {
public:
  RBFM_ScanIterator() {};
  ~RBFM_ScanIterator() {};

  // Never keep the results in the memory. When getNextRecord() is called, 
  // a satisfying record needs to be fetched from the file.
  // "data" follows the same format as RecordBasedFileManager::insertRecord().
  RC getNextRecord(RID &rid, void *data);
  RC close() 
  { 
	  fileHandle = NULL;
	  recordDescriptor.clear();
	  conditionField = 0;
	  compOp = CompOp::NO_OP;
	  free(value);
	  currentPageNum = 0;
	  currentSlotNum = -1;
	  maxPageNum = -1;
	  end = true; 
	  versionTable.clear();
	  attributeNames.clear();
	  return 0; 
  }

  PageNum currentPageNum;
  OffsetType currentSlotNum;

  void setMaxPageNum(int maxPageNum) { this->maxPageNum = maxPageNum; }
  bool getEnd() { return end; }
  void setEnd(bool end) { this->end = end; }
  void setConditionField(OffsetType conditionField) { this->conditionField = conditionField; }
  void setCompOp(const CompOp compOp) { this->compOp = compOp; }
  void setValue(const void* value) 
  { 
	  if (this->conditionField == -1)
	  {
		  this->value = NULL;
	  }
	  else
	  {
		  AttrType conditionType = this->recordDescriptor.at(this->conditionField).type;
		  if (conditionType == TypeInt)
		  {
			  this->value = malloc(sizeof(int));
			  memcpy(this->value, value, sizeof(int));
		  }
		  else if (conditionType == TypeReal)
		  {
			  this->value = malloc(sizeof(float));
			  memcpy(this->value, value, sizeof(float));
		  }
		  else if (conditionType == TypeVarChar)
		  {
			  int conditionStrLength;
			  memcpy(&conditionStrLength, value, sizeof(int));
			  this->value = malloc(sizeof(int) + conditionStrLength);
			  memcpy(this->value, value, sizeof(int));
			  memcpy((char*)this->value + sizeof(int), (char*)value + sizeof(int), conditionStrLength);
		  }
	  }
  }
  void setRecordDescriptor(const vector<Attribute> &recordDescriptor) { this->recordDescriptor = recordDescriptor; }
  void setFileHandle(FileHandle &fileHandle) { this->fileHandle = &fileHandle; }
  void setVersionTable(const vector<vector<Attribute>> &versionTable) { this->versionTable = versionTable; }
  void setAttributeNames(const vector<string> &attributeNames) { this->attributeNames = attributeNames; }

private:
	int maxPageNum;
	bool end;

	FileHandle* fileHandle;
	vector<Attribute> recordDescriptor;
	OffsetType conditionField;
	CompOp compOp;
	void *value;

	vector<vector<Attribute>> versionTable;
	vector<string> attributeNames;
};


class RecordBasedFileManager
{
public:
  static RecordBasedFileManager* instance();

  RC createFile(const string &fileName);
  
  RC destroyFile(const string &fileName);
  
  RC openFile(const string &fileName, FileHandle &fileHandle);
  
  RC closeFile(FileHandle &fileHandle);

  //  Format of the data passed into the function is the following:
  //  [n byte-null-indicators for y fields] [actual value for the first field] [actual value for the second field] ...
  //  1) For y fields, there is n-byte-null-indicators in the beginning of each record.
  //     The value n can be calculated as: ceil(y / 8). (e.g., 5 fields => ceil(5 / 8) = 1. 12 fields => ceil(12 / 8) = 2.)
  //     Each bit represents whether each field value is null or not.
  //     If k-th bit from the left is set to 1, k-th field value is null. We do not include anything in the actual data part.
  //     If k-th bit from the left is set to 0, k-th field contains non-null values.
  //     If there are more than 8 fields, then you need to find the corresponding byte first, 
  //     then find a corresponding bit inside that byte.
  //  2) Actual data is a concatenation of values of the attributes.
  //  3) For Int and Real: use 4 bytes to store the value;
  //     For Varchar: use 4 bytes to store the length of characters, then store the actual characters.
  //  !!! The same format is used for updateRecord(), the returned data of readRecord(), and readAttribute().
  // For example, refer to the Q8 of Project 1 wiki page.
  RC insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid);

  RC readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data);
  
  // This method will be mainly used for debugging/testing. 
  // The format is as follows:
  // field1-name: field1-value  field2-name: field2-value ... \n
  // (e.g., age: 24  height: 6.1  salary: 9000
  //        age: NULL  height: 7.5  salary: 7500)
  RC printRecord(const vector<Attribute> &recordDescriptor, const void *data);

/******************************************************************************************************************************************************************
IMPORTANT, PLEASE READ: All methods below this comment (other than the constructor and destructor) are NOT required to be implemented for the part 1 of the project
******************************************************************************************************************************************************************/
  RC deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid);

  // Assume the RID does not change after an update
  RC updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid);

  RC readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const string &attributeName, void *data);

  // Scan returns an iterator to allow the caller to go through the results one by one. 
  RC scan(FileHandle &fileHandle,
      const vector<Attribute> &recordDescriptor,
      const string &conditionAttribute,
      const CompOp compOp,                  // comparision type such as "<" and "="
      const void *value,                    // used in the comparison
      const vector<string> &attributeNames, // a list of projected attributes
      RBFM_ScanIterator &rbfm_ScanIterator);

public:
  RC appendDataInPage(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, int pageNum, const void *data, RID &rid, OffsetType &fieldInfoSize, char* &fieldInfo, OffsetType &dataSize);
  RC addDataInNewPage(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid, OffsetType &fieldInfoSize, char* &fieldInfo, OffsetType &dataSize);
  void generateFieldInfo(const vector<Attribute> &recordDescriptor, const void *data, OffsetType &resultLength, char* &result, OffsetType &dataSize);
  OffsetType generateSlotTable(char* &data, OffsetType &slotCount, OffsetType &slotSize);
  void moveSlots(const OffsetType targetOffset, const OffsetType startSlot, const OffsetType endSlot, char* &pageData);
  RC toFinalSlot(FileHandle &fileHandle, const RID &fromSlot, RID &finalSlot, char* &finalPage);

protected:
  RecordBasedFileManager();
  ~RecordBasedFileManager();

private:
  static RecordBasedFileManager *_rbf_manager;
};

#include "../rm/rm.h"

#endif
