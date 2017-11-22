#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>
#include <unordered_map>
#include <list>
#include <cmath>

#include "../rbf/rbfm.h"

# define IX_EOF (-1)  // end of the index scan

#define NULLNODE -1
#define UnknownNodeType 0
#define InternalNodeType 1
#define LeafNodeType 2

class IX_ScanIterator;
class IXFileHandle;
class BTree;
class Node;
class LeafEntry;
class LeafNode;
class IndexManager {

public:
	static IndexManager* instance();

	// Create an index file.
	RC createFile(const string &fileName);

	// Delete an index file.
	RC destroyFile(const string &fileName);

	// Open an index and return an ixfileHandle.
	RC openFile(const string &fileName, IXFileHandle &ixfileHandle);

	// Close an ixfileHandle for an index.
	RC closeFile(IXFileHandle &ixfileHandle);

	// Insert an entry into the given index that is indicated by the given ixfileHandle.
	RC insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

	// Delete an entry from the given index that is indicated by the given ixfileHandle.
	RC deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

	// Initialize and IX_ScanIterator to support a range search
	RC scan(IXFileHandle &ixfileHandle,
		const Attribute &attribute,
		const void *lowKey,
		const void *highKey,
		bool lowKeyInclusive,
		bool highKeyInclusive,
		IX_ScanIterator &ix_ScanIterator);

	// Print the B+ tree in pre-order (in a JSON record format)
	void printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const;

	int compareKey(AttrType attrType, const void* v1, const void* v2) const;
	RC refreshMetaData(IXFileHandle &ixfileHandle);

	void BSF(IXFileHandle &ixfileHandle,Node** cur_node,const Attribute& attribute,int height) const;
	void padding(int height) const;
	void print_leafkeys(AttrType attrType,Node** cur_node,int start,int end) const;
	void print_internalkeys(AttrType attrType,Node** cur_node,int index) const;
	int compareEntry(AttrType attrType,const LeafEntry &pair1, const LeafEntry &pair2) const;

protected:
	IndexManager();
	~IndexManager();

private:
	static IndexManager *_index_manager;
};

class IX_ScanIterator {
public:

	// Constructor
	IX_ScanIterator();

	// Destructor
	~IX_ScanIterator();

	// Get next matching entry
	RC getNextEntry(RID &rid, void *key);

	// Terminate index scan
	RC close();

	int compareKey(const void* v1, const void* v2);

public:
	AttrType attrType;
	LeafNode** currentNode;
	int previousIndex;
	RID previousRID;
	IXFileHandle *ixfileHandle;
	const void *lowKey;
	const void *highKey;
	bool lowKeyInclusive;
	bool highKeyInclusive;
	bool end;
	BTree* tree;
};

class IXFileHandle {
public:

	// variables to keep counter for each operation
	unsigned ixReadPageCounter;
	unsigned ixWritePageCounter;
	unsigned ixAppendPageCounter;
	int smallestLeaf;
	FileHandle handle;
	int root;
	BTree* tree;
	// Constructor
	IXFileHandle();

	RC readPage(PageNum pageNum, void *data);                             // Get a specific page
	RC writePage(PageNum pageNum, const void *data);                      // Write a specific page
	RC appendPage(const void *data);                                      // Append a specific page
	RC readMetaPage();
	RC writeMetaPage();
	// Destructor
	~IXFileHandle();

	// Put the current counter values of associated PF FileHandles into variables
	RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);
};

class Node
{
public:
	Node();
	virtual ~Node();

	bool isOverflow();
	bool isUnderflow();

public:
	MarkType nodeType;
	OffsetType nodeSize;
	Node** parentPointer;
	bool isDirty;
	PageNum pageNum;
	bool isLoaded;
};

class LeafEntry
{
public:
	void* key;
	RID rid;
	OffsetType size;

	LeafEntry();

	LeafEntry(const AttrType &attrType, const void* key, const RID rid);

	~LeafEntry();
};


class InternalEntry
{
public:
	void* key;
	Node** leftChild;
	Node** rightChild;

	InternalEntry();

	InternalEntry(const AttrType &attribute, const void* key, Node** leftChild, Node** rightChild);

	~InternalEntry();

	InternalEntry(const AttrType &attrType, const void* key);

private:
	OffsetType size;
};

class InternalNode : public Node
{
public:
	InternalNode();
	~InternalNode();

public:
	vector<InternalEntry> internalEntries;
};

class LeafNode : public Node
{
public:
	LeafNode();
	~LeafNode();

public:
	Node** rightPointer;
	LeafNode** overflowPointer;
	vector<LeafEntry> leafEntries;
};

class BTree
{
public:
	BTree();
	~BTree();
	Node** findLeafnode(IXFileHandle &ixfileHandle, const LeafEntry &pair, Node** cur_node);
	RC insertEntry(IXFileHandle &ixfileHandle, const LeafEntry &pair);
	RC deleteEntry(IXFileHandle &ixfileHandle, const LeafEntry &pair);
	char* generatePage(Node** node);
	Node** generateNode(char* data);
	RC findRecord(IXFileHandle &ixfileHandle, const LeafEntry &pair, vector<LeafEntry>::iterator &result);
	RC findLeaf(IXFileHandle &ixfileHandle, const LeafEntry &pair, LeafNode** &result);
	int compareKey(void* v1, void* v2);
	int compareEntry(const LeafEntry &pair1, const LeafEntry &pair2);
	RC loadNode(IXFileHandle &ixfileHandle, Node** &target);
	RC doDelete(IXFileHandle &ixfileHandle, Node** node, const LeafEntry &pair);
	RC removeEntryFromNode(Node** node, const LeafEntry &pair);
	OffsetType getEntrySize(int nodeType, const LeafEntry &pair, bool isLastEntry);
	RC adjustRoot(IXFileHandle &ixfileHandle);
	RC getNeighborIndex(Node** node, int &result);
	RC getNodesMergeSize(Node** node1, Node** node2, OffsetType sizeOfParentKey, OffsetType &result);
	OffsetType getKeySize(const void* key);
	RC mergeNodes(IXFileHandle &ixfileHandle, Node** node, Node** neighbor, int neighborIndex, int keyIndex, OffsetType mergedNodeSize);
	RC redistributeNodes(Node** node, Node** neighbor, int neighborIndex, int keyIndex, OffsetType keySize);
	RC refreshNodeSize(Node** node);
	RC writeNodesBack(IXFileHandle &ixfileHandle);
public:
	Node** root;
	Node** smallestLeaf;
	AttrType attrType;
	unordered_map<PageNum, Node**> nodeMap;
};

#endif
