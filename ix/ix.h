#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>

#include "../rbf/rbfm.h"

# define IX_EOF (-1)  // end of the index scan

#define NULLNODE -1
#define UnknownNodeType 0
#define InternalNodeType 1
#define LeafNodeType 2

class IX_ScanIterator;
class IXFileHandle;

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
};



class IXFileHandle {
    public:

    // variables to keep counter for each operation
    unsigned ixReadPageCounter;
    unsigned ixWritePageCounter;
    unsigned ixAppendPageCounter;

    // Constructor
    IXFileHandle();

    // Destructor
    ~IXFileHandle();

	// Put the current counter values of associated PF FileHandles into variables
	RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);

};

struct LeafEntry
{
	void* key;
	RID rid;

	LeafEntry()
	{
		this->key = NULL;
		rid.pageNum = -1;
		rid.slotNum = -1;
	}

	~LeafEntry()
	{
		delete this->key;
	}
};

struct InternalEntry
{
	void* key;
	Node* leftChild;
	Node* rightChild;

	InternalEntry()
	{
		this->key = NULL;
		this->leftChild = NULL;
		this->rightChild = NULL;
	}

	~InternalEntry()
	{
		delete this->key;
		delete this->leftChild;
		delete this->rightChild;
	}
};

class Node 
{
public:
	Node();
	~Node();

	bool isOverflow();
	bool isUnderflow();

public:
	MarkType nodeType;
	OffsetType nodeSize;
	Node* parentPointer;
	bool isDirty;
	PageNum pageNum;
	bool isLoaded;
};

class InternalNode: public Node 
{
public:
	vector<InternalEntry> internalEntries;
};

class LeafNode: public Node 
{
public:
	LeafNode();
	~LeafNode();

public:
	Node* rightPointer;
	Node* overflowPointer;
	vector<LeafEntry> leafEntries;
};

class BTree 
{
public:
	BTree();
	~BTree();

	void insertEntry(IXFileHandle &ixfileHandle, const LeafEntry pair);
	void deleteEntry(IXFileHandle &ixfileHandle, const LeafEntry pair);
	LeafNode* searchEntry(IXFileHandle &ixfileHandle, const LeafEntry pair);
	void traverse();
	char* generatePage(const Node* node);
	Node* generateNode(const char* data);
public:
	Node* root;
	Node* smallestLeaf;
	AttrType attrType;
};

#endif
