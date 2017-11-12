
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
    LeafEntry leafEntry={key,rid};
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
	delete this->parentPointer;
}

LeafNode::LeafNode()
{
	this->isDirty = false;
	this->isLoaded = false;
	this->nodeSize = 0;
	this->nodeType = LeafNodeType;
	this->overflowPointer = NULL;
	this->pageNum = -1;
	this->parentPointer = NULL;
	this->rightPointer = NULL;
}

LeafNode::~LeafNode()
{
	delete this->overflowPointer;
	delete this->parentPointer;
	delete this->rightPointer;
}

BTree::BTree()
{
	this->root = NULL;
	this->smallestLeaf = NULL;
	this->attrType = AttrType::TypeInt;
}

BTree::~BTree()
{
	delete this->root;
	delete this->smallestLeaf;
}
void BTree::insertEntry(IXFileHandle &ixfileHandle, const LeafEntry pair){
	if(root==NULL){
		//set node fields
		Node new_node=new LeafNode();   
		new_node.leafEntries.push_back(pair);
		new_node.nodeType=LeafNodeType;
		int entry_length;
		if(attrType==TypeVarChar){
			void * key1=pair.key;
			int var_length;
			memcpy(&var_length,(char *)pair.key,4);
			entry_length=4+var_length+8;
		}else{
			entry_length=12;
		}
		new_node.nodeSize=nodeSize+entry_length;
		//set tree fields
		root=*new_node;
		smallestLeaf=*new_node;
		void* new_page=malloc(PAGE_SIZE);
		new_page=generatePage(new_node);
		ixfileHandle.appendPage(new_page);
	}else{

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
	if (node->parentPointer)
	{
		PageNum parentPageNum = node->parentPointer->pageNum;
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
			PageNum leftChildPageNum = ((const InternalNode*)node)->internalEntries[i].leftChild->pageNum;
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
				PageNum rightChildPageNum = ((const InternalNode*)node)->internalEntries[i].rightChild->pageNum;
				memcpy(pageData + offset, &rightChildPageNum, sizeof(PageNum));
				offset += sizeof(PageNum);
			}
		}
	}
	else if (node->nodeType == LeafNodeType)
	{
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

Node* BTree::generateNode(const char* data)
{
	Node* result = NULL;
	MarkType nodeType;
	OffsetType offset = 0;
	memcpy(&nodeType, data + offset, sizeof(MarkType));
	offset += sizeof(MarkType);
	if (nodeType == InternalNodeType)
	{
		result = new InternalNode();
		OffsetType nodeSize;
		memcpy(&nodeSize, data + offset, sizeof(OffsetType));
		offset += sizeof(OffsetType);

	}
}

