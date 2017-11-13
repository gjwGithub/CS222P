
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
	LeafEntry leafEntry;
	leafEntry.key = &key;
	leafEntry.rid = rid;
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

LeafEntry::LeafEntry(const Attribute &attribute, void* key, RID rid)
{
	if (attribute.type == AttrType::TypeInt)
	{
		this->key = malloc(sizeof(int));
		memcpy(this->key, key, sizeof(int));
	}
	else if (attribute.type == AttrType::TypeReal)
	{
		this->key = malloc(sizeof(float));
		memcpy(this->key, key, sizeof(float));
	}
	else if (attribute.type == AttrType::TypeVarChar)
	{
		int strLength = *(int*)key;
		this->key = malloc(sizeof(int) + strLength);
		memcpy(this->key, key, sizeof(int) + strLength);
	}
	this->rid = rid;
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

RC BTree::insertEntry(IXFileHandle &ixfileHandle, const LeafEntry pair) {
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
		void* new_page = generatePage(*new_node);
		ixfileHandle.appendPage(new_page);
	}
	else {

	}

}

char* BTree::generatePage(const Node* node)
{
	char* pageData = (char*)calloc(PAGE_SIZE, 1);
	OffsetType offset = 0;
	memcpy(pageData + offset, &node->nodeType, sizeof(MarkType));
	offset += sizeof(MarkType);
	memcpy(pageData + offset, &node->nodeSize, sizeof(OffsetType));
	offset += sizeof(OffsetType);
	if (node->parentPointer && *node->parentPointer)
	{
		PageNum parentPageNum = (*node->parentPointer)->pageNum;
		memcpy(pageData + offset, &parentPageNum, sizeof(PageNum));
		offset += sizeof(PageNum);
	}
	else
	{
		PageNum parentPageNum = NULLNODE;
		memcpy(pageData + offset, &parentPageNum, sizeof(PageNum));
		offset += sizeof(PageNum);
	}
	if (node->nodeType == InternalNodeType)
	{
		OffsetType slotCount = (OffsetType)((const InternalNode*)node)->internalEntries.size();
		memcpy(pageData + PAGE_SIZE - sizeof(OffsetType), &slotCount, sizeof(OffsetType));
		for (OffsetType i = 0; i < slotCount; i++)
		{
			//If the slot is not the last slot, we only write its left child in page
			PageNum leftChildPageNum = (*((const InternalNode*)node)->internalEntries[i].leftChild)->pageNum;
			memcpy(pageData + offset, &leftChildPageNum, sizeof(PageNum));
			offset += sizeof(PageNum);
			//Store slot offsets in slot table
			memcpy(pageData + PAGE_SIZE - (i + 2) * sizeof(OffsetType), &offset, sizeof(OffsetType));
			//Store key of slot
			if (this->attrType == AttrType::TypeInt)
			{
				memcpy(pageData + offset, ((const InternalNode*)node)->internalEntries[i].key, sizeof(int));
				offset += sizeof(int);
			}
			else if (this->attrType == AttrType::TypeReal)
			{
				memcpy(pageData + offset, ((const InternalNode*)node)->internalEntries[i].key, sizeof(float));
				offset += sizeof(float);
			}
			else if (this->attrType == AttrType::TypeVarChar)
			{
				int strLength = *(int*)((const InternalNode*)node)->internalEntries[i].key;
				memcpy(pageData + offset, ((const InternalNode*)node)->internalEntries[i].key, sizeof(int) + strLength);
				offset += sizeof(int) + strLength;
			}
			//If the slot is the last slot, we will also write its right child in page
			if (i == slotCount - 1)
			{
				PageNum rightChildPageNum = (*((const InternalNode*)node)->internalEntries[i].rightChild)->pageNum;
				memcpy(pageData + offset, &rightChildPageNum, sizeof(PageNum));
				offset += sizeof(PageNum);
			}
		}
	}
	else if (node->nodeType == LeafNodeType)
	{
		memcpy(pageData + offset, &(*((const LeafNode*)node)->rightPointer)->pageNum, sizeof(PageNum));
		offset += sizeof(PageNum);
		memcpy(pageData + offset, &(*((const LeafNode*)node)->overflowPointer)->pageNum, sizeof(PageNum));
		offset += sizeof(PageNum);
		OffsetType slotCount = (OffsetType)((const LeafNode*)node)->leafEntries.size();
		memcpy(pageData + PAGE_SIZE - sizeof(OffsetType), &slotCount, sizeof(OffsetType));
		for (OffsetType i = 0; i < slotCount; i++)
		{
			//Store slot offsets in slot table
			memcpy(pageData + PAGE_SIZE - (i + 2) * sizeof(OffsetType), &offset, sizeof(OffsetType));
			//Store key of slot
			if (this->attrType == AttrType::TypeInt)
			{
				memcpy(pageData + offset, ((const LeafNode*)node)->leafEntries[i].key, sizeof(int));
				offset += sizeof(int);
			}
			else if (this->attrType == AttrType::TypeReal)
			{
				memcpy(pageData + offset, ((const LeafNode*)node)->leafEntries[i].key, sizeof(float));
				offset += sizeof(float);
			}
			else if (this->attrType == AttrType::TypeVarChar)
			{
				int strLength = *(int*)((const LeafNode*)node)->leafEntries[i].key;
				memcpy(pageData + offset, ((const LeafNode*)node)->leafEntries[i].key, sizeof(int) + strLength);
				offset += sizeof(int) + strLength;
			}
			//Store RID of slot
			memcpy(pageData + offset, &((const LeafNode*)node)->leafEntries[i].rid.pageNum, sizeof(PageNum));
			offset += sizeof(PageNum);
			memcpy(pageData + offset, &((const LeafNode*)node)->leafEntries[i].rid.slotNum, sizeof(unsigned));
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
				this->nodeMap[leftChildPageNum] = entry.leftChild;
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
				this->nodeMap[rightChildPageNum] = entry.rightChild;
			}

			(*(InternalNode**)result)->internalEntries.push_back(entry);
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

		PageNum rightPageNum;
		memcpy(&rightPageNum, data + offset, sizeof(PageNum));
		offset += sizeof(PageNum);
		if (rightPageNum != NULLNODE)
			(*(LeafNode**)result)->rightPointer = this->nodeMap[rightPageNum];
		else
			(*(LeafNode**)result)->rightPointer = NULL;

		PageNum overflowPageNum;
		memcpy(&overflowPageNum, data + offset, sizeof(PageNum));
		offset += sizeof(PageNum);
		if (rightPageNum != NULLNODE)
			(*(LeafNode**)result)->overflowPointer = (LeafNode**)this->nodeMap[overflowPageNum];
		else
			(*(LeafNode**)result)->overflowPointer = NULL;

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

			(*(LeafNode**)result)->leafEntries.push_back(entry);
		}
	}
	return result;
}

