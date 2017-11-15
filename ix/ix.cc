
#include "ix.h"

IndexManager* IndexManager::_index_manager = 0;

IndexManager* IndexManager::instance()
{
    if(!_index_manager)
        _index_manager = new IndexManager();

    return _index_manager;
}

IndexManager::IndexManager()
{
}

IndexManager::~IndexManager()
{
}

RC IndexManager::createFile(const string &fileName)
{
    return PagedFileManager::instance()->createFile(fileName);
}

RC IndexManager::destroyFile(const string &fileName)
{
    return PagedFileManager::instance()->destroyFile(fileName);
}

RC IndexManager::openFile(const string &fileName, IXFileHandle &ixfileHandle)
{
    return PagedFileManager::instance()->openFile(fileName, ixfileHandle.handle);
}

RC IndexManager::closeFile(IXFileHandle &ixfileHandle)
{
    return PagedFileManager::instance()->closeFile(ixfileHandle.handle);
}

RC IndexManager::insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
	LeafEntry leafEntry(attribute.type, key, rid);
    if(tree==NULL){
    	tree=new BTree();
    	tree->attrType=attribute.type;
    	RC rel=tree->insertEntry(ixfileHandle,leafEntry);
    	return rel;
    }else{
    	RC rel=tree->insertEntry(ixfileHandle,leafEntry);
    	return rel;
    }
}

RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
	if (this->tree) 
	{
		LeafEntry leafEntry(attribute.type, key, rid);
		return this->tree->deleteEntry(ixfileHandle, leafEntry);
	}
#ifdef DEBUG
	cerr << "The tree pointer is NULL when deleting entry." << endl;
#endif
    return -1;
}

RC IndexManager::scan(IXFileHandle &ixfileHandle,
        const Attribute &attribute,
        const void      *lowKey,
        const void      *highKey,
        bool			lowKeyInclusive,
        bool        	highKeyInclusive,
        IX_ScanIterator &ix_ScanIterator)
{
    return -1;
}

void IndexManager::printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const {
}

IX_ScanIterator::IX_ScanIterator()
{
}

IX_ScanIterator::~IX_ScanIterator()
{
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key)
{
    return -1;
}

RC IX_ScanIterator::close()
{
    return -1;
}


IXFileHandle::IXFileHandle()
{
    ixReadPageCounter = 0;
    ixWritePageCounter = 0;
    ixAppendPageCounter = 0;
}

IXFileHandle::~IXFileHandle()
{
}

RC IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
    return -1;
}

LeafEntry::LeafEntry(const AttrType &attrType, const void* key, const RID rid)
{
	if (attrType == AttrType::TypeInt)
	{
		this->key = malloc(sizeof(int));
		memcpy(this->key, key, sizeof(int));
	}
	else if (attrType == AttrType::TypeReal)
	{
		this->key = malloc(sizeof(float));
		memcpy(this->key, key, sizeof(float));
	}
	else if (attrType == AttrType::TypeVarChar)
	{
		int strLength = *(int*)key;
		this->key = malloc(sizeof(int) + strLength);
		memcpy(this->key, key, sizeof(int) + strLength);
	}
	this->rid = rid;
}

InternalEntry::InternalEntry(const AttrType &attrType, const void* key)
{
	if (attrType == AttrType::TypeInt)
	{
		this->key = malloc(sizeof(int));
		memcpy(this->key, key, sizeof(int));
	}
	else if (attrType == AttrType::TypeReal)
	{
		this->key = malloc(sizeof(float));
		memcpy(this->key, key, sizeof(float));
	}
	else if (attrType == AttrType::TypeVarChar)
	{
		int strLength = *(int*)key;
		this->key = malloc(sizeof(int) + strLength);
		memcpy(this->key, key, sizeof(int) + strLength);
	}
}

Node::Node()
{
	this->isDirty = false;
	this->isLoaded = false;
	this->nodeSize = 0;
	this->nodeType = LeafNodeType;
	this->pageNum = -1;
	this->parentPointer = NULL;
}

Node::~Node()
{
}

bool Node::isOverflow()
{
	return this->nodeSize > PAGE_SIZE;
}

bool Node::isUnderflow()
{
	return this->nodeSize < PAGE_SIZE / 2;
}

InternalNode::InternalNode()
{
	this->isDirty = false;
	this->isLoaded = false;
	this->nodeSize = sizeof(MarkType) + sizeof(OffsetType) + sizeof(PageNum) + sizeof(OffsetType);
	this->nodeType = InternalNodeType;
	this->pageNum = -1;
	this->parentPointer = NULL;
}

InternalNode::~InternalNode()
{
}

LeafNode::LeafNode()
{
	this->isDirty = false;
	this->isLoaded = false;
	this->nodeSize = sizeof(MarkType) + sizeof(OffsetType) + sizeof(PageNum) * 3 + sizeof(OffsetType);
	this->nodeType = LeafNodeType;
	this->overflowPointer = NULL;
	this->pageNum = -1;
	this->parentPointer = NULL;
	this->rightPointer = NULL;
}

LeafNode::~LeafNode()
{
}

BTree::BTree()
{
	this->root = NULL;
	this->smallestLeaf = NULL;
	this->attrType = AttrType::TypeInt;
}

BTree::~BTree()
{
	for (auto &item : this->nodeMap)
	{
		delete *item.second;
		delete item.second;
	}
}

RC BTree::insertEntry(IXFileHandle &ixfileHandle, const LeafEntry &pair) {
	if (root == NULL) {
		//set node fields
		Node** new_node = new Node*;
		*new_node = new LeafNode();
		((LeafNode*)*new_node)->leafEntries.push_back(pair);
		((LeafNode*)*new_node)->nodeType = LeafNodeType;
		int entry_length;
		if (attrType == TypeVarChar) {
			void * key1 = pair.key;
			int var_length;
			memcpy(&var_length, (char *)pair.key, 4);
			entry_length = 4 + var_length + 8;
		}
		else {
			entry_length = 12;
		}
		((LeafNode*)*new_node)->nodeSize += entry_length;
		//set tree fields
		root = new_node;
		smallestLeaf = new_node;
		void* new_page = generatePage(new_node);
		ixfileHandle.appendPage(new_page);
	}
	else {

	}

}

int BTree::compareKey(void* v1, void* v2)
{
	if (this->attrType == AttrType::TypeInt)
	{
		return memcmp(v1, v2, sizeof(int));
	}
	else if (this->attrType == AttrType::TypeReal)
	{
		return memcmp(v1, v2, sizeof(float));
	}
	else if (this->attrType == AttrType::TypeVarChar)
	{
		int strLength1 = *(int*)v1;
		int strLength2 = *(int*)v2;
		string s1((char*)v1 + sizeof(int), strLength1);
		string s2((char*)v2 + sizeof(int), strLength2);
		return s1.compare(s2);
	}
}

int BTree::compareEntry(const LeafEntry &pair1, const LeafEntry &pair2)
{
	int keyCompareResult = 0;
	if (this->attrType == AttrType::TypeInt)
	{
		keyCompareResult = memcmp(pair1.key, pair2.key, sizeof(int));
		if (keyCompareResult != 0)
			return keyCompareResult;
	}
	else if (this->attrType == AttrType::TypeReal)
	{
		keyCompareResult = memcmp(pair1.key, pair2.key, sizeof(float));
		if (keyCompareResult != 0)
			return keyCompareResult;
	}
	else if (this->attrType == AttrType::TypeVarChar)
	{
		int strLength1 = *(int*)pair1.key;
		int strLength2 = *(int*)pair2.key;
		string s1((char*)pair1.key + sizeof(int), strLength1);
		string s2((char*)pair2.key + sizeof(int), strLength2);
		keyCompareResult = s1.compare(s2);
		if (keyCompareResult != 0)
			return keyCompareResult; 
	}
	if (pair1.rid.pageNum - pair2.rid.pageNum != 0)
		return pair1.rid.pageNum - pair2.rid.pageNum;
	return pair1.rid.slotNum - pair2.rid.slotNum;
}

RC BTree::loadNode(IXFileHandle &ixfileHandle, Node** &target)
{
	//Pretend that we read file to load the node
	++ixfileHandle.ixReadPageCounter;

	PageNum loadPageNum = (*target)->pageNum;
	auto search = this->nodeMap.find(loadPageNum);
	if (search != this->nodeMap.end())
	{
		target = search->second;
	}
	else
	{
		char* pageData = (char*)malloc(PAGE_SIZE);
		if (ixfileHandle.readPage(loadPageNum, pageData) == -1)
		{
#ifdef DEBUG
			cerr << "Cannot read page " << loadPageNum << " when loading the node" << endl;
#endif
			free(pageData);
			return -1;
		}
		delete *target;
		delete target;
		target = generateNode(pageData);
		(*target)->pageNum = loadPageNum;
		this->nodeMap.insert(make_pair(loadPageNum, target));
		free(pageData);
	}
	return 0;
}

RC BTree::removeEntryFromNode(Node** node, const LeafEntry &pair)
{
	(*node)->isDirty = true;
	if ((*node)->nodeType == InternalNodeType)
	{
		for (auto i = ((InternalNode*)*node)->internalEntries.begin(); i != ((InternalNode*)*node)->internalEntries.end(); i++)
			if (compareKey(pair.key, i->key) == 0)
			{
				//Keep the last entry in root node left if we want to adjust the root
				if ((*node == *this->root && ((InternalNode*)*node)->internalEntries.size() > 1) || *node != *this->root)
					((InternalNode*)*node)->internalEntries.erase(i);
				OffsetType decreaseSize = getEntrySize(InternalNodeType, pair, ((InternalNode*)*node)->internalEntries.size() == 0);
				decreaseSize += sizeof(OffsetType); //The size of record offset in slot table
				return 0;
			}
#ifdef DEBUG
		cerr << "Cannot find the entry " << pair.rid.pageNum << ", " << pair.rid.slotNum << " when removing entry in an internal node" << endl;
#endif
		return -1;
	}
	else if ((*node)->nodeType == LeafNodeType)
	{
		for (auto i = ((LeafNode*)*node)->leafEntries.begin(); i != ((LeafNode*)*node)->leafEntries.end(); i++)
			if (compareEntry(pair, *i) == 0)
			{
				((LeafNode*)*node)->leafEntries.erase(i);
				OffsetType decreaseSize = getEntrySize(LeafNodeType, pair, ((LeafNode*)*node)->leafEntries.size() == 0);
				decreaseSize += sizeof(OffsetType); //The size of record offset in slot table
				return 0;
			}
#ifdef DEBUG
		cerr << "Cannot find the entry " << pair.rid.pageNum << ", " << pair.rid.slotNum << " when removing entry in an leaf node" << endl;
#endif
		return -1;
	}
}

OffsetType BTree::getEntrySize(int nodeType, const LeafEntry &pair, bool isLastEntry)
{
	if (nodeType == InternalNodeType)
	{
		OffsetType result = sizeof(PageNum); //size of left child page num
		if (isLastEntry)
			result += sizeof(PageNum);
		if (this->attrType == AttrType::TypeInt)
		{
			result += sizeof(int);
		}
		else if (this->attrType == AttrType::TypeReal)
		{
			result += sizeof(float);
		}
		else if (this->attrType == AttrType::TypeVarChar)
		{
			int strLength = *(int*)pair.key;
			result += sizeof(int) + strLength;
		}
		return result;
	}
	else if (nodeType == LeafNodeType)
	{
		OffsetType result = sizeof(PageNum) + sizeof(unsigned); //size of rid
		if (this->attrType == AttrType::TypeInt)
		{
			result += sizeof(int);
		}
		else if (this->attrType == AttrType::TypeReal)
		{
			result += sizeof(float);
		}
		else if (this->attrType == AttrType::TypeVarChar)
		{
			int strLength = *(int*)pair.key;
			result += sizeof(int) + strLength;
		}
		return result;
	}
}

//Returns the leaf containing the given key.
RC BTree::findLeaf(IXFileHandle &ixfileHandle, const LeafEntry &pair, LeafNode** &result)
{
	Node** currentNode = this->root;
	if (currentNode == NULL) 
	{
#ifdef DEBUG
		cerr << "The root pointer is NULL when finding the leaf" << endl;
#endif
		return -1;
	}
	if (*currentNode == NULL)
	{
#ifdef DEBUG
		cerr << "The root pointer has been released before when finding the leaf" << endl;
#endif
		return -1;
	}
	while ((*currentNode)->nodeType == InternalNodeType) 
	{
		int i = 0;
		while (i < ((InternalNode*)*currentNode)->internalEntries.size())
		{
			if (compareKey(((InternalNode*)*currentNode)->internalEntries[i].key, pair.key) < 0)
				++i;
			else
				break;
		}
		if (((InternalNode*)*currentNode)->internalEntries[i].leftChild)
		{
			if (*((InternalNode*)*currentNode)->internalEntries[i].leftChild)
			{
				if ((*((InternalNode*)*currentNode)->internalEntries[i].leftChild)->isLoaded)
				{
					currentNode = ((InternalNode*)*currentNode)->internalEntries[i].leftChild;
				}
				else
				{
					//If the node was not loaded before, we need to load it from page file
					if (loadNode(ixfileHandle, ((InternalNode*)*currentNode)->internalEntries[i].leftChild) == -1)
					{
#ifdef DEBUG
						cerr << "Cannot load the left child node when finding the leaf" << endl;
#endif
						return -1;
					}
					currentNode = ((InternalNode*)*currentNode)->internalEntries[i].leftChild;
				}
			}
			else
			{
#ifdef DEBUG
				cerr << "The leftchild points to a released memory when finding the leaf" << endl;
#endif
				return -1;
			}
		}
		else
		{
#ifdef DEBUG
			cerr << "The leftchild is null when finding the leaf" << endl;
#endif
			return -1;
		}
	}
	result = (LeafNode**)currentNode;
	return;
}

//Finds and returns the record to which a key refers.
RC BTree::findRecord(IXFileHandle &ixfileHandle, const LeafEntry &pair, LeafEntry* &result)
{
	LeafNode** leaf = NULL;
	if (findLeaf(ixfileHandle, pair, leaf) == -1)
	{
#ifdef DEBUG
		cerr << "Cannot find the leaf node when finding record, rid = " << pair.rid.pageNum << ", " << pair.rid.slotNum << endl;
#endif
		return -1;
	}
	if (leaf == NULL)
	{
#ifdef DEBUG
		cerr << "The result leaf is NULL in finding record" << endl;
#endif
		return -1;
	}
	size_t i = 0;
	for (; i < (*leaf)->leafEntries.size(); i++)
		if (compareEntry((*leaf)->leafEntries[i], pair) == 0)
			break;
	if (i == (*leaf)->leafEntries.size())
	{
#ifdef DEBUG
		cerr << "Cannot find the record in the leaf node when finding record, rid = " << pair.rid.pageNum << ", " << pair.rid.slotNum << endl;
#endif
		return -1;
	}
	result = &(*leaf)->leafEntries[i];
	return 0;
}

RC BTree::adjustRoot(IXFileHandle &ixfileHandle)
{
	//Case: nonempty root. Key and pointer have already been deleted, so nothing to be done.
	if ((*this->root)->nodeType == InternalNodeType && ((InternalNode*)*this->root)->internalEntries.size() > 0)
		return 0;
	if ((*this->root)->nodeType == LeafNodeType && ((LeafNode*)*this->root)->leafEntries.size() > 0)
		return 0;
	//Case: empty root.
	if ((*this->root)->nodeType == InternalNodeType)
	{
		// If it has a child, promote the first (only) child as the new root.
		Node** temp = this->root;
		this->nodeMap.erase((*this->root)->pageNum);
		this->root = ((InternalNode*)*this->root)->internalEntries[0].rightChild;
		(*this->root)->parentPointer = NULL;
		ixfileHandle.root = (*this->root)->pageNum;
		delete *temp;
		delete temp;
	}
	else
	{
		// If it is a leaf (has no children), then the whole tree is empty.
		delete *this->root;
		delete this->root;
		this->root = NULL;
	}
	return 0;
}

//Get the index of the left neighbor of the node
//If the node is the leftmost child of parent, return index = -1
RC BTree::getNeighborIndex(Node** node, int &result)
{
	size_t internalEntriesSize = ((InternalNode*)((*node)->parentPointer))->internalEntries.size();
	for (size_t i = 0; i < internalEntriesSize; i++)
		if (*((InternalNode*)((*node)->parentPointer))->internalEntries[i].leftChild == *node)
		{
			result = i - 1;
			return 0;
		}
	if (*((InternalNode*)((*node)->parentPointer))->internalEntries[internalEntriesSize - 1].rightChild == *node)
	{
		result = internalEntriesSize - 1;
		return 0;
	}
#ifdef DEBUG
	cerr << "Cannot find the left neighbor of the node when getting neighbor index" << endl;
#endif
	return -1;
}

int BTree::getKeySize(const void* key)
{
	if (this->attrType == AttrType::TypeInt)
		return sizeof(int);
	else if (this->attrType == AttrType::TypeReal)
		return sizeof(float);
	else if (this->attrType == AttrType::TypeInt)
	{
		int strLength = *(int*)key;
		return sizeof(int) + strLength;
	}
}

RC BTree::getNodesMergeSize(Node** node1, Node** node2, int sizeOfParentKey, OffsetType &result)
{
	if ((*node1)->nodeType == InternalNodeType && (*node2)->nodeType == InternalNodeType)
	{
		size_t node2EntriesSize = (*node1)->nodeSize;
		node2EntriesSize -= sizeof(MarkType) - sizeof(OffsetType) - sizeof(int); //Decrease size of nodeType, usedSpace, and parent pageNum
		node2EntriesSize -= sizeof(int); //Derease size of a pointer in node2
		node2EntriesSize -= sizeof(OffsetType); //Decrease size of slotCount in slot table
		result = (*node1)->nodeSize + node2EntriesSize;
		result += sizeof(int) + sizeOfParentKey + sizeof(int); //We need to add the key between nodes' pointers from their parent node
		return 0;
	}
	else if ((*node1)->nodeType == LeafNodeType && (*node2)->nodeType == LeafNodeType)
	{
		size_t node2EntriesSize = (*node1)->nodeSize;
		node2EntriesSize -= sizeof(MarkType) - sizeof(OffsetType) - sizeof(int) * 3; //Decrease size of nodeType, usedSpace, parent pageNum, right pageNum, and overflow PageNum
		node2EntriesSize -= sizeof(OffsetType); //Decrease size of slotCount in slot table
		result = (*node1)->nodeSize + node2EntriesSize;
		return 0;
	}
#ifdef DEBUG
	cerr << "Cannot get merge size with two different types of nodes" << endl;
#endif
	return -1;
}

RC BTree::mergeNodes(IXFileHandle &ixfileHandle, Node** node, Node** neighbor, int neighborIndex, int keyIndex, int keySize, int mergedNodeSize)
{
	//Swap neighbor with node if node is on the extreme left and neighbor is to its right.
	if (neighborIndex == -1) 
	{
		Node** tmp = node;
		node = neighbor;
		neighbor = tmp;
	}

	if ((*node)->nodeType == InternalNodeType && (*neighbor)->nodeType == InternalNodeType)
	{
		//Append the key in parent to neighbor node
		InternalEntry entryFromParent(this->attrType, ((InternalNode*)*(*node)->parentPointer)->internalEntries[keyIndex].key);
		entryFromParent.leftChild = ((InternalNode*)*neighbor)->internalEntries.back().rightChild;
		entryFromParent.rightChild = ((InternalNode*)*node)->internalEntries.front().leftChild;
		((InternalNode*)*neighbor)->internalEntries.push_back(entryFromParent);

		for (auto& i : ((InternalNode*)*node)->internalEntries)
		{
			InternalEntry entryFromNode(this->attrType, i.key);
			entryFromNode.leftChild = i.leftChild;
			entryFromNode.rightChild = i.rightChild;
			//All children must now point up to the same parent.
			(*entryFromNode.leftChild)->parentPointer = neighbor;
			(*entryFromNode.rightChild)->parentPointer = neighbor;
			((InternalNode*)*neighbor)->internalEntries.push_back(entryFromNode);
		}

		//Update the size of neighbor node
		(*neighbor)->nodeSize = mergedNodeSize;
		(*neighbor)->isDirty = true;
	}
	else if ((*node)->nodeType == LeafNodeType && (*neighbor)->nodeType == LeafNodeType)
	{
		for (auto& i : ((LeafNode*)*node)->leafEntries)
		{
			LeafEntry entryFromNode(this->attrType, i.key, i.rid);
			((LeafNode*)*neighbor)->leafEntries.push_back(entryFromNode);
		}

		//Update the size of neighbor node
		(*neighbor)->nodeSize = mergedNodeSize;
		(*neighbor)->isDirty = true;
	}
	else
	{
#ifdef DEBUG
		cerr << "The type of node and that of neighbor are different when merging nodes" << endl;
#endif
		return -1;
	}
	RID dummyRid;
	dummyRid.pageNum = -1;
	dummyRid.slotNum = -1;
	LeafEntry pair(this->attrType, ((InternalNode*)*(*node)->parentPointer)->internalEntries[keyIndex].key, dummyRid);
	if (doDelete(ixfileHandle, (*node)->parentPointer, pair) == -1)
	{
#ifdef DEBUG
		cerr << "Cannot delete the parent node when merging nodes" << endl;
#endif
		return -1;
	}

	//Remove node from memory
	this->nodeMap.erase((*node)->pageNum);
	delete *node;
	delete node;
	return 0;
}

//Deletes an entry from the B+ tree
RC BTree::doDelete(IXFileHandle &ixfileHandle, Node** node, const LeafEntry &pair)
{
	//Remove entry from node.
	if (removeEntryFromNode(node, pair) == -1)
	{
#ifdef DEBUG
		cerr << "Cannot remove the entry when deleting entry, rid = " << pair.rid.pageNum << ", " << pair.rid.slotNum << endl;
#endif
		return -1;
	}
	//Case: deletion from the root.
	if (*node == *this->root)
		return adjustRoot(ixfileHandle);
	//Case: node stays at or above minimum.
	if (!(*node)->isUnderflow())
		return 0;
	//Case: node falls below minimum. Either merge or redistribution is needed.
	//Find the appropriate neighbor node with which to merge.
	int neighborIndex;
	if (getNeighborIndex(node, neighborIndex) == -1)
	{
#ifdef DEBUG
		cerr << "Cannot get the neighbor node index when deleting entry, rid = " << pair.rid.pageNum << ", " << pair.rid.slotNum << endl;
#endif
		return -1;
	}
	//The index of key which between the pointer to the neighbor and the pointer to the node
	int keyIndex = neighborIndex == -1 ? 0 : neighborIndex;
	Node** neighbor = neighborIndex == -1 ? ((InternalNode*)*(*node)->parentPointer)->internalEntries[0].rightChild : ((InternalNode*)*(*node)->parentPointer)->internalEntries[neighborIndex].leftChild;
	int keySize = getKeySize(((InternalNode*)*(*node)->parentPointer)->internalEntries[keyIndex].key);
	OffsetType sizeOfMergeNodes;
	if (getNodesMergeSize((Node**)node, neighbor, keySize, sizeOfMergeNodes) == -1)
	{
#ifdef DEBUG
		cerr << "Cannot get size of the merged node when deleting entry, rid = " << pair.rid.pageNum << ", " << pair.rid.slotNum << endl;
#endif
		return -1;
	}
	if (sizeOfMergeNodes <= PAGE_SIZE)
	{

	}
}

//Master deletion function
RC BTree::deleteEntry(IXFileHandle &ixfileHandle, const LeafEntry &pair)
{
	LeafEntry* leafRecord = NULL;
	if (findRecord(ixfileHandle, pair, leafRecord) == -1)
	{
#ifdef DEBUG
		cerr << "Cannot find the record in the leaf node when deleting entry, rid = " << pair.rid.pageNum << ", " << pair.rid.slotNum << endl;
#endif
		return -1;
	}
	LeafNode** leaf = NULL;
	if (findLeaf(ixfileHandle, pair, leaf) == -1)
	{
#ifdef DEBUG
		cerr << "Cannot find the leaf node when deleting entry, rid = " << pair.rid.pageNum << ", " << pair.rid.slotNum << endl;
#endif
		return -1;
	}
	if (leafRecord != NULL && leaf != NULL)
	{
		if (doDelete(ixfileHandle, (Node**)leaf, pair) == -1)
		{
#ifdef DEBUG
			cerr << "Cannot delete the leaf node when deleting entry, rid = " << pair.rid.pageNum << ", " << pair.rid.slotNum << endl;
#endif
			return -1;
		}
	}
	else
	{
#ifdef DEBUG
		cerr << "Cannot find the target when deleting entry, rid = " << pair.rid.pageNum << ", " << pair.rid.slotNum << endl;
#endif
		return -1;
	}
	return 0;
}

char* BTree::generatePage(const Node** node)
{
	char* pageData = (char*)calloc(PAGE_SIZE, 1);
	OffsetType offset = 0;
	memcpy(pageData + offset, &(*node)->nodeType, sizeof(MarkType));
	offset += sizeof(MarkType);
	memcpy(pageData + offset, &(*node)->nodeSize, sizeof(OffsetType));
	offset += sizeof(OffsetType);
	if ((*node)->parentPointer && *(*node)->parentPointer)
	{
		PageNum parentPageNum = (*(*node)->parentPointer)->pageNum;
		memcpy(pageData + offset, &parentPageNum, sizeof(PageNum));
		offset += sizeof(PageNum);
	}
	else
	{
		PageNum parentPageNum = NULLNODE;
		memcpy(pageData + offset, &parentPageNum, sizeof(PageNum));
		offset += sizeof(PageNum);
	}
	if ((*node)->nodeType == InternalNodeType)
	{
		OffsetType slotCount = (OffsetType)((const InternalNode*)*node)->internalEntries.size();
		memcpy(pageData + PAGE_SIZE - sizeof(OffsetType), &slotCount, sizeof(OffsetType));
		for (OffsetType i = 0; i < slotCount; i++)
		{
			//If the slot is not the last slot, we only write its left child in page
			PageNum leftChildPageNum = (*((const InternalNode*)*node)->internalEntries[i].leftChild)->pageNum;
			memcpy(pageData + offset, &leftChildPageNum, sizeof(PageNum));
			offset += sizeof(PageNum);
			//Store slot offsets in slot table
			memcpy(pageData + PAGE_SIZE - (i + 2) * sizeof(OffsetType), &offset, sizeof(OffsetType));
			//Store key of slot
			if (this->attrType == AttrType::TypeInt)
			{
				memcpy(pageData + offset, ((const InternalNode*)*node)->internalEntries[i].key, sizeof(int));
				offset += sizeof(int);
			}
			else if (this->attrType == AttrType::TypeReal)
			{
				memcpy(pageData + offset, ((const InternalNode*)*node)->internalEntries[i].key, sizeof(float));
				offset += sizeof(float);
			}
			else if (this->attrType == AttrType::TypeVarChar)
			{
				int strLength = *(int*)((const InternalNode*)*node)->internalEntries[i].key;
				memcpy(pageData + offset, ((const InternalNode*)*node)->internalEntries[i].key, sizeof(int) + strLength);
				offset += sizeof(int) + strLength;
			}
			//If the slot is the last slot, we will also write its right child in page
			if (i == slotCount - 1)
			{
				PageNum rightChildPageNum = (*((const InternalNode*)*node)->internalEntries[i].rightChild)->pageNum;
				memcpy(pageData + offset, &rightChildPageNum, sizeof(PageNum));
				offset += sizeof(PageNum);
			}
		}
	}
	else if ((*node)->nodeType == LeafNodeType)
	{
		memcpy(pageData + offset, &(*((const LeafNode*)*node)->rightPointer)->pageNum, sizeof(PageNum));
		offset += sizeof(PageNum);
		memcpy(pageData + offset, &(*((const LeafNode*)*node)->overflowPointer)->pageNum, sizeof(PageNum));
		offset += sizeof(PageNum);
		OffsetType slotCount = (OffsetType)((const LeafNode*)*node)->leafEntries.size();
		memcpy(pageData + PAGE_SIZE - sizeof(OffsetType), &slotCount, sizeof(OffsetType));
		for (OffsetType i = 0; i < slotCount; i++)
		{
			//Store slot offsets in slot table
			memcpy(pageData + PAGE_SIZE - (i + 2) * sizeof(OffsetType), &offset, sizeof(OffsetType));
			//Store key of slot
			if (this->attrType == AttrType::TypeInt)
			{
				memcpy(pageData + offset, ((const LeafNode*)*node)->leafEntries[i].key, sizeof(int));
				offset += sizeof(int);
			}
			else if (this->attrType == AttrType::TypeReal)
			{
				memcpy(pageData + offset, ((const LeafNode*)*node)->leafEntries[i].key, sizeof(float));
				offset += sizeof(float);
			}
			else if (this->attrType == AttrType::TypeVarChar)
			{
				int strLength = *(int*)((const LeafNode*)*node)->leafEntries[i].key;
				memcpy(pageData + offset, ((const LeafNode*)*node)->leafEntries[i].key, sizeof(int) + strLength);
				offset += sizeof(int) + strLength;
			}
			//Store RID of slot
			memcpy(pageData + offset, &((const LeafNode*)*node)->leafEntries[i].rid.pageNum, sizeof(PageNum));
			offset += sizeof(PageNum);
			memcpy(pageData + offset, &((const LeafNode*)*node)->leafEntries[i].rid.slotNum, sizeof(unsigned));
			offset += sizeof(unsigned);
		}
	}
	return pageData;
}

Node** BTree::generateNode(const char* data)
{
	Node** result = new Node*;
	MarkType nodeType;
	OffsetType offset = 0;
	memcpy(&nodeType, data + offset, sizeof(MarkType));
	offset += sizeof(MarkType);
	if (nodeType == InternalNodeType)
	{
		*result = new InternalNode();
		(*result)->isLoaded = true;

		OffsetType nodeSize;
		memcpy(&nodeSize, data + offset, sizeof(OffsetType));
		offset += sizeof(OffsetType);
		(*result)->nodeSize = nodeSize;

		PageNum parent;
		memcpy(&parent, data + offset, sizeof(PageNum));
		offset += sizeof(PageNum);
		if (parent != NULLNODE)
			(*result)->parentPointer = this->nodeMap[parent];
		else
			(*result)->parentPointer = this->nodeMap[parent];

		OffsetType slotCount;
		memcpy(&slotCount, data + PAGE_SIZE - sizeof(OffsetType), sizeof(OffsetType));
		for (OffsetType i = 0; i < slotCount; i++)
		{
			OffsetType slotOffset;
			memcpy(&slotOffset, data + PAGE_SIZE - sizeof(OffsetType) * (i + 2), sizeof(OffsetType));
			InternalEntry entry;
			OffsetType keyLength = 0;
			if (this->attrType = AttrType::TypeInt)
			{
				entry.key = malloc(sizeof(int));
				memcpy(entry.key, data + slotOffset, sizeof(int));
				keyLength = sizeof(int);
			}
			else if (this->attrType = AttrType::TypeReal)
			{
				entry.key = malloc(sizeof(float));
				memcpy(entry.key, data + slotOffset, sizeof(float));
				keyLength = sizeof(float);
			}
			else if (this->attrType = AttrType::TypeVarChar)
			{
				int strLength = *(int*)(data + slotOffset);
				memcpy(entry.key, data + slotOffset, sizeof(int) + strLength);
				keyLength = sizeof(int) + strLength;
			}

			//Generate left child pointer
			OffsetType leftChildPointerOffset = slotOffset - sizeof(PageNum);
			PageNum leftChildPageNum = *(PageNum*)(data + leftChildPointerOffset);
			auto search = this->nodeMap.find(leftChildPageNum);
			if (search != this->nodeMap.end())
			{
				entry.leftChild = this->nodeMap[leftChildPageNum];
			}
			else 
			{
				Node** newNodePointer = new Node*;
				*newNodePointer = new Node();
				entry.leftChild = newNodePointer;
				(*entry.leftChild)->pageNum = leftChildPageNum;
			}

			//Generate right child pointer
			OffsetType rightChildPointerOffset = slotOffset + keyLength;
			PageNum rightChildPageNum = *(PageNum*)(data + rightChildPointerOffset);
			auto search = this->nodeMap.find(rightChildPageNum);
			if (search != this->nodeMap.end())
			{
				entry.rightChild = this->nodeMap[rightChildPageNum];
			}
			else
			{
				Node** newNodePointer = new Node*;
				*newNodePointer = new Node();
				entry.rightChild = newNodePointer;
				(*entry.rightChild)->pageNum = rightChildPageNum;
			}

			((InternalNode*)*result)->internalEntries.push_back(entry);
		}
	}
	else if (nodeType == LeafNodeType)
	{
		*result = new LeafNode();
		(*result)->isLoaded = true;

		OffsetType nodeSize;
		memcpy(&nodeSize, data + offset, sizeof(OffsetType));
		offset += sizeof(OffsetType);
		(*result)->nodeSize = nodeSize;

		PageNum parent;
		memcpy(&parent, data + offset, sizeof(PageNum));
		offset += sizeof(PageNum);
		if (parent != NULLNODE)
			(*result)->parentPointer = this->nodeMap[parent];
		else
			(*result)->parentPointer = NULL;

		PageNum rightPageNum;
		memcpy(&rightPageNum, data + offset, sizeof(PageNum));
		offset += sizeof(PageNum);
		if (rightPageNum != NULLNODE)
		{
			auto search = this->nodeMap.find(rightPageNum);
			if (search != this->nodeMap.end())
			{
				((LeafNode*)*result)->rightPointer = this->nodeMap[rightPageNum];
			}
			else
			{
				Node** newNodePointer = new Node*;
				*newNodePointer = new Node();
				((LeafNode*)*result)->rightPointer = newNodePointer;
				(*((LeafNode*)*result)->rightPointer)->pageNum = rightPageNum;
			}
		}
		else
			((LeafNode*)*result)->rightPointer = NULL;

		PageNum overflowPageNum;
		memcpy(&overflowPageNum, data + offset, sizeof(PageNum));
		offset += sizeof(PageNum);
		if (overflowPageNum != NULLNODE)
			((LeafNode*)*result)->overflowPointer = (LeafNode**)this->nodeMap[overflowPageNum];
		else
			((LeafNode*)*result)->overflowPointer = NULL;

		OffsetType slotCount;
		memcpy(&slotCount, data + PAGE_SIZE - sizeof(OffsetType), sizeof(OffsetType));
		for (OffsetType i = 0; i < slotCount; i++)
		{
			OffsetType slotOffset;
			memcpy(&slotOffset, data + PAGE_SIZE - sizeof(OffsetType) * (i + 2), sizeof(OffsetType));
			LeafEntry entry;
			OffsetType keyLength = 0;
			if (this->attrType = AttrType::TypeInt)
			{
				entry.key = malloc(sizeof(int));
				memcpy(entry.key, data + slotOffset, sizeof(int));
				keyLength = sizeof(int);
			}
			else if (this->attrType = AttrType::TypeReal)
			{
				entry.key = malloc(sizeof(float));
				memcpy(entry.key, data + slotOffset, sizeof(float));
				keyLength = sizeof(float);
			}
			else if (this->attrType = AttrType::TypeVarChar)
			{
				int strLength = *(int*)(data + slotOffset);
				memcpy(entry.key, data + slotOffset, sizeof(int) + strLength);
				keyLength = sizeof(int) + strLength;
			}
			memcpy(&entry.rid.pageNum, data + slotOffset + keyLength, sizeof(PageNum));
			memcpy(&entry.rid.slotNum, data + slotOffset + keyLength + sizeof(PageNum), sizeof(unsigned));

			((LeafNode*)*result)->leafEntries.push_back(entry);
		}
	}
	return result;
}

