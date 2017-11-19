
#include "ix.h"

IndexManager* IndexManager::_index_manager = 0;

IndexManager* IndexManager::instance()
{
	if (!_index_manager)
		_index_manager = new IndexManager();

	return _index_manager;
}

IndexManager::IndexManager()
{
	tree=NULL;
}

IndexManager::~IndexManager()
{
	delete tree;
}

RC IndexManager::createFile(const string &fileName)
{
	RC status = PagedFileManager::instance()->createFile(fileName);
	if (status)
	{
#ifdef DEBUG
		cerr << "Cannot create the file while creating file" << endl;
#endif
		return -1;
	}
	FILE *file = fopen(fileName.c_str(), "w");
	if (file == NULL)
	{
#ifdef DEBUG
		cerr << "File does not exist!" << endl;
#endif
		return -1;
	}
	unsigned ixReadPageCounter = 0;
	unsigned ixWritePageCounter = 0;
	unsigned ixAppendPageCounter = 0;
	int root = -1;
	int smallestLeaf = -1;
	int pageCount = 0;
	char* metaData = (char*)calloc(PAGE_SIZE, 1);
	OffsetType offset = 0;
	memcpy(metaData + offset, &ixReadPageCounter, sizeof(unsigned));
	offset += sizeof(unsigned);
	memcpy(metaData + offset, &ixWritePageCounter, sizeof(unsigned));
	offset += sizeof(unsigned);
	memcpy(metaData + offset, &ixAppendPageCounter, sizeof(unsigned));
	offset += sizeof(unsigned);
	memcpy(metaData + offset, &root, sizeof(int));
	offset += sizeof(int);
	memcpy(metaData + offset, &smallestLeaf, sizeof(int));
	offset += sizeof(int);
	memcpy(metaData + offset, &pageCount, sizeof(int));
	offset += sizeof(int);
	size_t writeSize = fwrite(metaData, 1, PAGE_SIZE, file);
	if (writeSize != PAGE_SIZE)
	{
#ifdef DEBUG
		cerr << "Only write " << writeSize << " bytes, less than PAGE_SIZE " << PAGE_SIZE << " bytes in creating metadata" << endl;
#endif
		free(metaData);
		return -1;
	}
	status = fflush(file);
	if (status)
	{
#ifdef DEBUG
		cerr << "Cannot flush the file while creating metadata" << endl;
#endif
		free(metaData);
		return -1;
	}
	free(metaData);
	status = fclose(file);
	if (status)
	{
#ifdef DEBUG
		cerr << "Cannot close the file while creating the file" << endl;
#endif
		return -1;
	}
	return 0;

}

RC IXFileHandle::readMetaPage() {
	if (!handle.file) {
#ifdef DEBUG
		cerr << "File was not open while reading meta data" << endl;
#endif
		return -1;
	}
	int status = fseek(handle.file, 0, SEEK_SET);
	if (status)
	{
#ifdef DEBUG
		cerr << "fseek error in reading meta data " << endl;
#endif
		return -1;
	}
	char* data = (char*)malloc(PAGE_SIZE);
	size_t readSize = fread(data, 1, PAGE_SIZE, handle.file);
	if (readSize != PAGE_SIZE)
	{
#ifdef DEBUG
		cerr << "Only read " << readSize << " bytes, less than PAGE_SIZE " << PAGE_SIZE << " bytes in reading meta data " << endl;
#endif
		free(data);
		return -1;
	}

	OffsetType offset = 0;
	memcpy(&(this->ixReadPageCounter), data + offset, sizeof(unsigned));
	offset += sizeof(unsigned);
	memcpy(&(this->ixWritePageCounter), data + offset, sizeof(unsigned));
	offset += sizeof(unsigned);
	memcpy(&(this->ixAppendPageCounter), data + offset, sizeof(unsigned));
	offset += sizeof(unsigned);
	memcpy(&(this->root), data + offset, sizeof(int));
	offset += sizeof(int);
	memcpy(&(this->smallestLeaf), data + offset, sizeof(int));
	offset += sizeof(int);
	memcpy(&(this->handle.pageCount), data + offset, sizeof(int));
	offset += sizeof(int);
	free(data);
	return 0;
}

RC IXFileHandle::writeMetaPage() {
	if (!handle.file) {
#ifdef DEBUG
		cerr << "File was not open while reading meta data" << endl;
#endif
		return -1;
	}
	int status = fseek(handle.file, 0, SEEK_SET);
	if (status)
	{
#ifdef DEBUG
		cerr << "fseek error in reading meta data " << endl;
#endif
		return -1;
	}
	char* data = (char*)calloc(PAGE_SIZE, 1);
	OffsetType offset = 0;
	memcpy(data + offset, &(this->ixReadPageCounter), sizeof(unsigned));
	offset += sizeof(unsigned);
	memcpy(data + offset, &(this->ixWritePageCounter), sizeof(unsigned));
	offset += sizeof(unsigned);
	memcpy(data + offset, &(this->ixAppendPageCounter), sizeof(unsigned));
	offset += sizeof(unsigned);
	memcpy(data + offset, &(this->root), sizeof(int));
	offset += sizeof(int);
	memcpy(data + offset, &(this->smallestLeaf), sizeof(int));
	offset += sizeof(int);
	memcpy(data + offset, &(this->handle.pageCount), sizeof(int));
	offset += sizeof(int);
	size_t writeSize = fwrite(data, 1, PAGE_SIZE, handle.file);
	if (writeSize != PAGE_SIZE)
	{
#ifdef DEBUG
		cerr << "Only write " << writeSize << " bytes, less than PAGE_SIZE " << PAGE_SIZE << " bytes in writing meta data " << endl;
#endif
		free(data);
		return -1;
	}
	status = fflush(handle.file);
	if (status)
	{
#ifdef DEBUG
		cerr << "Cannot flush the file in writing meta data " << endl;
#endif
		free(data);
		return -1;
	}
	free(data);
	return 0;
}

RC IndexManager::destroyFile(const string &fileName)
{
	return PagedFileManager::instance()->destroyFile(fileName);
}

RC IndexManager::openFile(const string &fileName, IXFileHandle &ixfileHandle)
{
	if (PagedFileManager::instance()->openFile(fileName, ixfileHandle.handle) == -1)
	{
#ifdef DEBUG
		cerr << "Open file error in open file" << endl;
#endif
		return -1;
	}
	if (ixfileHandle.readMetaPage() == -1)
	{
#ifdef DEBUG
		cerr << "Read metadata error in open file" << endl;
#endif
		return -1;
	}
	return 0;
}

RC IndexManager::refreshMetaData(IXFileHandle &ixfileHandle)
{
	if (this->tree)
	{
		if (this->tree->root && (*this->tree->root))
			ixfileHandle.root = (*this->tree->root)->pageNum;
		else
			ixfileHandle.root = NULLNODE;
		if (this->tree->smallestLeaf && (*this->tree->smallestLeaf))
			ixfileHandle.smallestLeaf = (*this->tree->smallestLeaf)->pageNum;
		else
			ixfileHandle.smallestLeaf = NULLNODE;
	}
	return 0;
}

RC IndexManager::closeFile(IXFileHandle &ixfileHandle)
{
	refreshMetaData(ixfileHandle);
	if (ixfileHandle.writeMetaPage() == -1)
		return -1;
	if (PagedFileManager::instance()->closeFile(ixfileHandle.handle) == -1)
		return -1;
	return 0;
}

RC IndexManager::insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{

	LeafEntry leafEntry(attribute.type, key, rid);
	if(tree == NULL) {
		tree = new BTree();
		tree->attrType = attribute.type;
		cout<<"finish tree"<<endl;
	}
	ixfileHandle.readMetaPage();
	if(ixfileHandle.root==-1){
		tree->root==NULL;
	}
	cout<<"come to tree insert"<<endl;
	cout<<"                                 root                        "<<ixfileHandle.root<<endl;
	RC rel = tree->insertEntry(ixfileHandle, leafEntry);
	return rel;

}

RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
	LeafEntry leafEntry(attribute.type, key, rid);
	if (this->tree)
	{
		return this->tree->deleteEntry(ixfileHandle, leafEntry);
	}
	else
	{
		this->tree = new BTree();
		tree->attrType = attribute.type;

		char *rootPage = (char*)malloc(PAGE_SIZE);
		ixfileHandle.readPage(ixfileHandle.root, rootPage);
		this->tree->root = this->tree->generateNode(rootPage);
		(*this->tree->root)->pageNum = ixfileHandle.root;
		free(rootPage);
		this->tree->nodeMap.insert(make_pair(ixfileHandle.root, this->tree->root));

		if (ixfileHandle.root == ixfileHandle.smallestLeaf)
		{
			this->tree->smallestLeaf = this->tree->root;
		}
		else
		{
			char *smallestLeafPage = (char*)malloc(PAGE_SIZE);
			ixfileHandle.readPage(ixfileHandle.smallestLeaf, smallestLeafPage);
			this->tree->smallestLeaf = this->tree->generateNode(smallestLeafPage);
			(*this->tree->smallestLeaf)->pageNum = ixfileHandle.smallestLeaf;
			free(smallestLeafPage);
			this->tree->nodeMap.insert(make_pair(ixfileHandle.smallestLeaf, this->tree->smallestLeaf));
		}

		return this->tree->deleteEntry(ixfileHandle, leafEntry);
	}
	return 0;
}

RC IXFileHandle::readPage(PageNum pageNum, void *data) {
	ixReadPageCounter++;
	RC rel = handle.readPage(pageNum, data);
	return rel;
}

RC IXFileHandle::writePage(PageNum pageNum, const void *data) {
	ixWritePageCounter++;
	RC rel = handle.writePage(pageNum, data);
	return rel;
}

RC IXFileHandle::appendPage(const void *data) {
	ixAppendPageCounter++;
	RC rel = handle.appendPage(data);
	return rel;
}

int IndexManager::compareKey(AttrType attrType, const void* v1, const void* v2)
{
	if (attrType == AttrType::TypeInt)
	{
		return *(int*)v1 - *(int*)v2;
	}
	else if (attrType == AttrType::TypeReal)
	{
		return *(float*)v1 - *(float*)v2;
	}
	else if (attrType == AttrType::TypeVarChar)
	{
		int strLength1 = *(int*)v1;
		int strLength2 = *(int*)v2;
		string s1((char*)v1 + sizeof(int), strLength1);
		string s2((char*)v2 + sizeof(int), strLength2);
		return s1.compare(s2);
	}
}

RC IndexManager::scan(IXFileHandle &ixfileHandle,
	const Attribute &attribute,
	const void      *lowKey,
	const void      *highKey,
	bool			lowKeyInclusive,
	bool        	highKeyInclusive,
	IX_ScanIterator &ix_ScanIterator)
{
	ix_ScanIterator.ixfileHandle = &ixfileHandle;
	ix_ScanIterator.attrType = attribute.type;
	ix_ScanIterator.lowKey = lowKey;
	ix_ScanIterator.highKey = highKey;
	ix_ScanIterator.lowKeyInclusive = lowKeyInclusive;
	ix_ScanIterator.highKeyInclusive = highKeyInclusive;
	ix_ScanIterator.end = false;

	if (this->tree == NULL)
	{
		this->tree = new BTree();
		tree->attrType = attribute.type;

		char *rootPage = (char*)malloc(PAGE_SIZE);
		ixfileHandle.readPage(ixfileHandle.root, rootPage);
		this->tree->root = this->tree->generateNode(rootPage);
		(*this->tree->root)->pageNum = ixfileHandle.root;
		free(rootPage);
		this->tree->nodeMap.insert(make_pair(ixfileHandle.root, this->tree->root));

		if (ixfileHandle.smallestLeaf == ixfileHandle.root)
		{
			this->tree->smallestLeaf = this->tree->root;
		}
		else
		{
			char *smallestLeafPage = (char*)malloc(PAGE_SIZE);
			ixfileHandle.readPage(ixfileHandle.smallestLeaf, smallestLeafPage);
			this->tree->smallestLeaf = this->tree->generateNode(smallestLeafPage);
			(*this->tree->smallestLeaf)->pageNum = ixfileHandle.smallestLeaf;
			free(smallestLeafPage);
			this->tree->nodeMap.insert(make_pair(ixfileHandle.smallestLeaf, this->tree->smallestLeaf));
		}
	}
	ix_ScanIterator.tree = this->tree;
	if (lowKey == NULL)
	{
		ix_ScanIterator.previousIndex = -1;
		ix_ScanIterator.currentNode = (LeafNode**)this->tree->smallestLeaf;
	}
	else
	{
		RID dummyRid;
		LeafEntry leafEntry(attribute.type, lowKey, dummyRid);
		LeafNode** result = NULL;
		if (this->tree->findLeaf(ixfileHandle, leafEntry, result) == -1)
		{
#ifdef DEBUG
			cerr << "Cannot find the leaf node when scanning" << endl;
#endif
			return -1;
		}
		bool foundKey = false;
		while (!foundKey)
		{
			for (size_t i = 0; i < (*result)->leafEntries.size(); i++)
			{
				if (lowKeyInclusive)
				{
					if (compareKey(attribute.type, lowKey, (*result)->leafEntries[i].key) <= 0)
					{
						foundKey = true;
						ix_ScanIterator.previousIndex = i - 1;
						ix_ScanIterator.currentNode = result;
						break;
					}
				}
				else
				{
					if (compareKey(attribute.type, lowKey, (*result)->leafEntries[i].key) < 0)
					{
						foundKey = true;
						ix_ScanIterator.previousIndex = i - 1;
						ix_ScanIterator.currentNode = result;
						break;
					}
				}
			}
			if (!foundKey)
			{
				if ((*result)->rightPointer && *(*result)->rightPointer)
				{
					if (!(*(*result)->rightPointer)->isLoaded)
					{
						//If the node was not loaded before, we need to load it from page file
						if (this->tree->loadNode(ixfileHandle, (*result)->rightPointer) == -1)
						{
#ifdef DEBUG
							cerr << "Cannot load the right pointer node when scanning" << endl;
#endif
							return -1;
						}
					}
					result = (LeafNode**)((*result)->rightPointer);
				}
				else
				{
					ix_ScanIterator.end = true;
					break;
				}
			}
		}
	}
	return 0;
}

void IndexManager::printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const {
	// if(tree==NULL){
	// 	tree=new BTree();
	// }
	// Node** root_node ;
	// void* data = malloc(PAGE_SIZE);
	// ixfileHandle.readPage(ixfileHandle.root, data);
	// root_node = tree->generateNode((char *)data);

	Node** root_node;  // Initiat root node
	auto iter=tree->nodeMap.find(ixfileHandle.root);
	if (iter != tree->nodeMap.end()) {
		root_node = iter->second;
	}
	else {
		void* data = malloc(PAGE_SIZE);
		ixfileHandle.readPage(ixfileHandle.root, data);
		root_node = tree->generateNode((char *)data);
		tree->nodeMap.insert(make_pair(ixfileHandle.root, root_node));
		free(data);
	}

	BSF(ixfileHandle,root_node,attribute,0);
}
void IndexManager::BSF(IXFileHandle &ixfileHandle,Node** cur_node,const Attribute& attribute,int height) const{

	if ((*cur_node)->isLoaded == false){
		void* data = malloc(PAGE_SIZE);
		ixfileHandle.readPage((*cur_node)->pageNum, data);
		cur_node = tree->generateNode((char *)data);
	}
	padding(height);
	cout<<"{";
	if((*cur_node)->pageNum == ixfileHandle.root){
		cout<<endl;
		padding(height);
	}
	cout<< "\"keys\":";
	cout<< "[";
	if((*cur_node)->nodeType==LeafNodeType){
		int start=0;
		LeafEntry first_entry=((LeafNode*)*cur_node)->leafEntries[0];
		for(int i=0;i<=((LeafNode*)*cur_node)->leafEntries.size();i++){
			if(i==((LeafNode*)*cur_node)->leafEntries.size()){
				print_leafkeys(attribute.type,cur_node,start,i-1);
			}else{
				if(compareEntry(attribute.type,first_entry,((LeafNode*)*cur_node)->leafEntries[i])==0);
				else{
					print_leafkeys(attribute.type,cur_node,start,i-1);
					start=i;
					first_entry=((LeafNode*)*cur_node)->leafEntries[i];
					if(i != ((LeafNode*)*cur_node)->leafEntries.size());
					cout<<",";
				}
			}
			cout<<endl;
		}
		padding(height);
		cout<<"]";
	}else{
	//generate the cur_node key
		for(int i=0;i < ((InternalNode*)*cur_node)->internalEntries.size();i++){
			print_internalkeys(attribute.type,cur_node,i);
			if(i != ((InternalNode*)*cur_node)->internalEntries.size()-1)
				cout<<",";
			cout<<endl;
		}
		padding(height);
		cout<<"]";
		//generate the cur_node children
		cout<<","<<endl;
		padding(height);
		cout<< "\"children\":[" << endl;

		for(int i=0;i< ((InternalNode*)*cur_node)->internalEntries .size();i++){
			BSF(ixfileHandle,((InternalNode*)*cur_node)->internalEntries[i].leftChild,attribute,height+1);
			cout<<",";
			if(i == ((InternalNode*)*cur_node)->internalEntries.size()-1){
				BSF(ixfileHandle,((InternalNode*)*cur_node)->internalEntries[i].rightChild,attribute,height+1);
			}
			cout<<endl;
		}
		padding(height);
		cout<<"]";
	}
	if((*cur_node)->pageNum == ixfileHandle.root){
		cout<<endl;
		padding(height);
	}
	cout<<"}";

	
}
void IndexManager::padding(int height) const{
	for(int i=0; i<height;i++){
		cout<<"  ";
	}
}
void IndexManager::print_leafkeys(AttrType attrType,Node** cur_node,int start,int end) const{
	cout<<'"';
	if(attrType!=TypeVarChar){
		int key1;
		memcpy(&key1,(char *)(((LeafNode*)*cur_node)->leafEntries[start].key),4);
		cout<<key1<<":[";
	}else{
		int length;
		memcpy(&length,((LeafNode*)*cur_node)->leafEntries[start].key,sizeof(int));
		void* str=malloc(length+4);
		memcpy((char *)str,((LeafNode*)*cur_node)->leafEntries[start].key,length+4);
		cout<<str<<":[";
	}
	for(int i=start;i<=end;i++){
		cout << "(" << ((LeafNode*)*cur_node)->leafEntries[i].rid.pageNum << "," << ((LeafNode*)*cur_node)->leafEntries[i].rid.slotNum << ")";
	}
	cout<<"]";
	cout << '"';
}
void IndexManager::print_internalkeys(AttrType attrType,Node** cur_node,int index) const{
	cout<<'"';
	if(attrType!=TypeVarChar){
		int key1;
		memcpy(&key1,((InternalNode*)*cur_node)->internalEntries[index].key,sizeof(int));
		cout<<key1<<'"';
	}else{
		int length;
		memcpy(&length,((InternalNode*)*cur_node)->internalEntries[index].key,sizeof(int));
		void* str=malloc(length+4);
		memcpy((char *)str,((InternalNode*)*cur_node)->internalEntries[index].key,length+4);
		cout<<str<<'"';
	}

}

int IndexManager::compareEntry(AttrType attrType,const LeafEntry &pair1, const LeafEntry &pair2) const{
	int keyCompareResult = 0;
	if (attrType == AttrType::TypeInt)
	{
		keyCompareResult = memcmp(pair1.key, pair2.key, sizeof(int));
		if (keyCompareResult != 0)
			return keyCompareResult;
	}
	else if (attrType == AttrType::TypeReal)
	{
		keyCompareResult = memcmp(pair1.key, pair2.key, sizeof(float));
		if (keyCompareResult != 0)
			return keyCompareResult;
	}
	else if (attrType == AttrType::TypeVarChar)
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

IX_ScanIterator::IX_ScanIterator()
{
	this->attrType = AttrType::TypeInt;
	this->currentNode = NULL;
	this->previousIndex = -1;
	this->previousRID = RID();
	this->ixfileHandle = NULL;
	this->lowKey = NULL;
	this->highKey = NULL;
	this->lowKeyInclusive = false;
	this->highKeyInclusive = false;
	this->end = false;
	this->tree = NULL;
}

IX_ScanIterator::~IX_ScanIterator()
{
}

int IX_ScanIterator::compareKey(const void* v1, const void* v2)
{
	if (this->attrType == AttrType::TypeInt)
	{
		return *(int*)v1 - *(int*)v2;
	}
	else if (this->attrType == AttrType::TypeReal)
	{
		return *(float*)v1 - *(float*)v2;
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

RC IX_ScanIterator::getNextEntry(RID &rid, void *key)
{
	if (!this->end)
	{
		//If it is the rightmost leaf node
		if (this->currentNode == NULL || *this->currentNode == NULL)
		{
			this->end = true;
			return IX_EOF;
		}
		int currentIndex = this->previousIndex + 1;
		//Adjust the index if delete scan happens
		if (this->previousIndex != -1)
		{
			if ((*this->currentNode)->leafEntries[previousIndex].rid.pageNum != this->previousRID.pageNum || (*this->currentNode)->leafEntries[previousIndex].rid.slotNum != this->previousRID.slotNum)
				--currentIndex;
		}
		if (this->highKey)
		{
			//If current key exceeds the high key
			if (this->highKeyInclusive && compareKey((*this->currentNode)->leafEntries[currentIndex].key, this->highKey) > 0)
			{
				this->end = true;
				return IX_EOF;
			}
			if (!this->highKeyInclusive && compareKey((*this->currentNode)->leafEntries[currentIndex].key, this->highKey) >= 0)
			{
				this->end = true;
				return IX_EOF;
			}
		}
		rid = (*this->currentNode)->leafEntries[currentIndex].rid;
		key = (*this->currentNode)->leafEntries[currentIndex].key;
		//If the current key is the last key in leaf node
		if (currentIndex == (*this->currentNode)->leafEntries.size() - 1)
		{
			//Whether the current node is the rightmost leaf node
			if ((*currentNode)->rightPointer == NULL || *(*currentNode)->rightPointer == NULL)
			{
				this->end = true;
				return 0;
			}
			else
			{
				if (!(*(*currentNode)->rightPointer)->isLoaded)
				{
					//If the node was not loaded before, we need to load it from page file
					if (this->tree->loadNode(*this->ixfileHandle, (*currentNode)->rightPointer) == -1)
					{
#ifdef DEBUG
						cerr << "Cannot load the right pointer node when getting next entry" << endl;
#endif
						return -1;
					}
				}
				this->currentNode = (LeafNode**)(*currentNode)->rightPointer;
				this->previousIndex = -1;
				this->previousRID = RID();
			}
		}
		else
		{
			this->previousIndex = currentIndex;
			this->previousRID = (*this->currentNode)->leafEntries[currentIndex].rid;
		}
		return 0;
	}
	return IX_EOF;
}

RC IX_ScanIterator::close()
{
	this->end = true;
	return 0;
}

IXFileHandle::IXFileHandle()
{
	ixReadPageCounter = 0;
	ixWritePageCounter = 0;
	ixAppendPageCounter = 0;
	root = NULLNODE;
	smallestLeaf = NULLNODE;
}

IXFileHandle::~IXFileHandle()
{
}

RC IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
	readPageCount = ixReadPageCounter;
	writePageCount = ixWritePageCounter;
	appendPageCount = ixAppendPageCounter;
	return 0;
}

LeafEntry::LeafEntry(const AttrType &attrType, const void* key, const RID rid)
{
	if (attrType == AttrType::TypeInt)
	{
		this->key = malloc(sizeof(int));
		memcpy(this->key, key, sizeof(int));
		this->size = sizeof(int);
	}
	else if (attrType == AttrType::TypeReal)
	{
		this->key = malloc(sizeof(float));
		memcpy(this->key, key, sizeof(float));
		this->size = sizeof(float);
	}
	else if (attrType == AttrType::TypeVarChar)
	{
		int strLength = *(int*)key;
		this->key = malloc(sizeof(int) + strLength);
		memcpy(this->key, key, sizeof(int) + strLength);
		this->size = sizeof(int) + strLength;
	}
	this->rid = rid;
}

LeafEntry::LeafEntry(const LeafEntry &entry)
{
	this->key = malloc(entry.size);
	memcpy(this->key, entry.key, entry.size);
	this->size = entry.size;
	this->rid = entry.rid;
}

InternalEntry::InternalEntry(const AttrType &attribute, const void* key, Node** leftChild, Node** rightChild)
{
	if (attribute == AttrType::TypeInt)
	{
		this->key = malloc(sizeof(int));
		memcpy(this->key, key, sizeof(int));
	}
	else if (attribute == AttrType::TypeReal)
	{
		this->key = malloc(sizeof(float));
		memcpy(this->key, key, sizeof(float));
	}
	else if (attribute == AttrType::TypeVarChar)
	{
		int strLength = *(int*)key;
		this->key = malloc(sizeof(int) + strLength);
		memcpy(this->key, key, sizeof(int) + strLength);
	}
	this->leftChild = leftChild;
	this->rightChild = rightChild;
}

InternalEntry::InternalEntry(const AttrType &attrType, const void* key)
{
	if (attrType == AttrType::TypeInt)
	{
		this->key = malloc(sizeof(int));
		this->size = sizeof(int);
		memcpy(this->key, key, sizeof(int));
	}
	else if (attrType == AttrType::TypeReal)
	{
		this->key = malloc(sizeof(float));
		this->size = sizeof(float);
		memcpy(this->key, key, sizeof(float));
	}
	else if (attrType == AttrType::TypeVarChar)
	{
		int strLength = *(int*)key;
		this->key = malloc(sizeof(int) + strLength);
		this->size = sizeof(int) + strLength;
		memcpy(this->key, key, sizeof(int) + strLength);
	}
}

InternalEntry::InternalEntry(const InternalEntry &entry)
{
	this->key = malloc(entry.size);
	memcpy(this->key, entry.key, entry.size);
	this->size = entry.size;
	this->leftChild = entry.leftChild;
	this->rightChild = entry.rightChild;
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
	if (*this->parentPointer == NULL)
		delete this->parentPointer;
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
	if (*this->parentPointer == NULL)
		delete this->parentPointer;
	if (*this->rightPointer == NULL)
		delete this->rightPointer;
	if (*this->overflowPointer == NULL)
		delete this->overflowPointer;
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
		if (*item.second)
			delete *item.second;
		delete item.second;
	}
}

RC BTree::insertEntry(IXFileHandle &ixfileHandle, const LeafEntry &pair) {
	//if (root == NULL) {
		//set node fields
		// Node** new_node = new Node*;
		// *new_node = new LeafNode();
		// ((LeafNode*)*new_node)->leafEntries.push_back(pair);
		// ((LeafNode*)*new_node)->nodeType = LeafNodeType;
		int entry_length;

		if (attrType == TypeVarChar) {
			int var_length;
			memcpy(&var_length, (char *)pair.key, 4);

		}
		else {
			entry_length = 12;

		}

		if (root == NULL) {  //insert into the first node in the new tree
			//set node fields
			cout<<"create root"<<endl;
			Node** new_node = new Node*;
			*new_node = new LeafNode();
			((LeafNode*)*new_node)->leafEntries.push_back(pair);
			((LeafNode*)*new_node)->nodeType = LeafNodeType;
			((LeafNode*)*new_node)->nodeSize += entry_length + sizeof(OffsetType);
			((LeafNode*)*new_node)->isLoaded = true;
			//set tree fields
			root = new_node;
			smallestLeaf = new_node;
			void* new_page = generatePage(new_node);
			ixfileHandle.appendPage(new_page);
			nodeMap.insert(make_pair(ixfileHandle.ixAppendPageCounter - 1, new_node));
			((LeafNode*)*new_node)->pageNum = ixfileHandle.ixAppendPageCounter - 1;
			//set meta fields
			ixfileHandle.root = ixfileHandle.ixAppendPageCounter - 1;
			ixfileHandle.smallestLeaf = ixfileHandle.ixAppendPageCounter - 1;
			ixfileHandle.writeMetaPage();
		}
		else {
			cout<<"root exists"<<endl;
			Node** root_node;  // Initiat root node
			unordered_map<PageNum, Node**>::iterator iter; //judge the curnode exist in the hashmap
			iter = nodeMap.find(ixfileHandle.root);
			if (iter != nodeMap.end()) {
				root_node = iter->second;
			}
			else {
				void* data = malloc(PAGE_SIZE);
				ixfileHandle.readPage(ixfileHandle.root, data);
				root_node = generateNode((char *)data);
				nodeMap.insert(make_pair(ixfileHandle.root, root_node));
				free(data);
			}
			cout<<"1"<<endl;
			Node** tar_leafNode = findLeafnode(ixfileHandle, pair, root_node);     //find the inserted leaf node
			cout<<"2"<<endl;
			if ((*(LeafNode**)tar_leafNode)->isLoaded == false) {
				void* data = malloc(PAGE_SIZE);
				PageNum cur_pageNum = (*(LeafNode**)tar_leafNode)->pageNum;
				ixfileHandle.readPage(cur_pageNum, data);
				tar_leafNode = generateNode((char *)data);
				nodeMap.insert(make_pair(cur_pageNum, tar_leafNode));
				free(data);
			}
			cout<<"3"<<endl;
			//first insert the entry to the origin leafnode
			vector<LeafEntry>::iterator iter_vector = (*(LeafNode**)tar_leafNode)->leafEntries.begin();   //find the right position in leafnode
			int isadded = 0;
			for (int i = 0; i < (*(LeafNode**)tar_leafNode)->leafEntries.size(); i++) {
				if (compareEntry(pair, (*(LeafNode**)tar_leafNode)->leafEntries[i])<0) {
					cout<<"7"<<endl;
					cout<<"insert"<<*(int *)pair.key<<endl;
					cout<<" i"<<i<<endl;
					cout<<"before"<<*(int *)((*(LeafNode**)tar_leafNode)->leafEntries[i].key)<<endl;
					(*(LeafNode**)tar_leafNode)->leafEntries.insert(iter_vector + i, pair);
					isadded = 1;
					break;
				}
			}
			cout<<"4"<<endl;
			if (isadded == 0){
				cout<<"insert"<<*(int *)pair.key<<endl;
				(*(LeafNode**)tar_leafNode)->leafEntries.push_back(pair);
			}
			(*(LeafNode**)tar_leafNode)->nodeSize += entry_length + sizeof(OffsetType);
			cout<<"size"<<(*(LeafNode**)tar_leafNode)->nodeSize<<endl;
			if ((*(LeafNode**)tar_leafNode)->nodeSize <= PAGE_SIZE) {   //insert into node into space-enough node
				//write the updated origin leaf node into file
				void* new_page = generatePage(tar_leafNode);
				ixfileHandle.writePage((*(LeafNode**)tar_leafNode)->pageNum, new_page);
				free(new_page);
				cout<<"6"<<endl;
				return 0;
			}
			else {                                                        //split node;
				Node** new_node = new Node*;
				*new_node = new LeafNode();
				((LeafNode*)*new_node)->isLoaded = true;
				//move the origin leafnode the second half records to the new leafnode, and update the nodesize
				for (int i = (*(LeafNode**)tar_leafNode)->leafEntries.size() / 2; i < (*(LeafNode**)tar_leafNode)->leafEntries.size(); i++) {
					((LeafNode*)*new_node)->leafEntries.push_back((*(LeafNode**)tar_leafNode)->leafEntries[i]);
					int var_length;
					if (attrType == TypeVarChar) {
						memcpy(&var_length, (char *)((LeafNode*)*new_node)->leafEntries[i].key, 4);
						var_length += 12;
					}
					else {
						var_length = 12;
					}
					(*(LeafNode**)tar_leafNode)->nodeSize -= var_length + sizeof(OffsetType);
					((LeafNode*)*new_node)->nodeSize += var_length + sizeof(OffsetType);
				}
				cout<<"11"<<endl;
				//the mid_node key's length
				int mid_index = (*(LeafNode**)tar_leafNode)->leafEntries.size() / 2;
				cout<<"mid"<<mid_index<<endl;
				int var_length;
				void* key_mid;
				if (attrType == TypeVarChar) {
					memcpy(&var_length, (char *)((LeafNode*)*tar_leafNode)->leafEntries[mid_index].key, 4);
					key_mid = malloc(sizeof(int) + var_length);
					memcpy((char *)key_mid, (char *)((LeafNode*)*tar_leafNode)->leafEntries[mid_index].key, sizeof(int) + var_length);
				}
				else {
					var_length = 4;
					key_mid = malloc(sizeof(int));
					memcpy((char *)key_mid, (char *)((LeafNode*)*tar_leafNode)->leafEntries[mid_index].key, sizeof(int));
				}
				cout<<"22"<<endl;
				//delete the second half records from the origin leafnode.
				//for (int i = mid_index; i < (*(LeafNode**)tar_leafNode)->leafEntries.size(); i++) {
					(*(LeafNode**)tar_leafNode)->leafEntries.erase((*(LeafNode**)tar_leafNode)->leafEntries.begin() + mid_index,(*(LeafNode**)tar_leafNode)->leafEntries.end());
				//}
				cout<<"33"<<endl;
				//update the origin and new node's rightpointer
				((LeafNode*)*new_node)->rightPointer = (*(LeafNode**)tar_leafNode)->rightPointer;
				(*(LeafNode**)tar_leafNode)->rightPointer = new_node;
				((LeafNode*)*new_node)->nodeType = LeafNodeType;
				cout<<"44"<<endl;
				//if it dont have the parentNode,create a parentNode
				if (((*(LeafNode**)tar_leafNode)->parentPointer) == NULL) {
					cout<<"                                    create new root                    "<<endl;
					((LeafNode*)*new_node)->pageNum = ixfileHandle.ixAppendPageCounter + 1;  //assume the new leaf node will append in ixappendpageCounter th
					InternalEntry internal_pair(attrType, ((LeafNode*)*new_node)->leafEntries[0].key, tar_leafNode, new_node);
					Node** new_parentNode = new Node*;
					*new_parentNode = new InternalNode();
					((InternalNode*)*new_parentNode)->isLoaded = true;
					((InternalNode*)*new_parentNode)->internalEntries.push_back(internal_pair);
					((InternalNode*)*new_parentNode)->nodeType = InternalNodeType;
					((InternalNode*)*new_parentNode)->nodeSize += var_length + 2 * sizeof(int) + sizeof(OffsetType);
					//write the father node to the file
					cout<<"66"<<endl;
					void* new_page = generatePage(new_parentNode);
					ixfileHandle.appendPage(new_page);
					nodeMap.insert(make_pair(ixfileHandle.ixAppendPageCounter - 1, new_parentNode));
					((InternalNode*)*new_parentNode)->pageNum = ixfileHandle.ixAppendPageCounter - 1;
					free(new_page);
					cout<<"77"<<endl;
					//set father-son relatoinship.
					(*(LeafNode**)tar_leafNode)->parentPointer = new_parentNode;
					((LeafNode*)*new_node)->parentPointer = new_parentNode;
					//set meta fields
					cout<<"88"<<endl;
					ixfileHandle.root = ixfileHandle.ixAppendPageCounter - 1;
					ixfileHandle.writeMetaPage();
					cout<<"                                new root page                       "<<ixfileHandle.root<<endl;
					//update the origin leafnode to the file
					cout<<"99"<<endl;
					void* new_originLeafpage = generatePage(tar_leafNode);
					int originLeafpage = (*(LeafNode**)tar_leafNode)->pageNum;
					ixfileHandle.writePage(originLeafpage, new_originLeafpage);
					free(new_originLeafpage);
					//add the updated leafnode to the file

					void* new_newLeafpage = generatePage(new_node);
					ixfileHandle.appendPage(new_newLeafpage);
					nodeMap.insert(make_pair(ixfileHandle.ixAppendPageCounter - 1, new_node));
					((LeafNode*)*new_node)->pageNum = ixfileHandle.ixAppendPageCounter - 1;
					free(new_newLeafpage);
				}
				else {  // it have the parentNode
					cout<<"00"<<endl;
					Node** origin_parentNode = new Node*;
					PageNum origin_parentNode_pagenum = ((InternalNode*)*((*(LeafNode**)tar_leafNode)->parentPointer))->pageNum;
					cout<<"parent page num"<<origin_parentNode_pagenum;
					iter = nodeMap.find(origin_parentNode_pagenum);
					if (iter != nodeMap.end()) {
						origin_parentNode = iter->second;
					}
					else {
						cout << "not found" << endl;
					}
					//update the origin leafnode to the file

					void* new_originLeafpage = generatePage(tar_leafNode);
					int originLeafpage = (*(LeafNode**)tar_leafNode)->pageNum;
					ixfileHandle.writePage(originLeafpage, new_originLeafpage);
					free(new_originLeafpage);
					//add the updated leafnode to the file
					((LeafNode*)*new_node)->parentPointer = origin_parentNode;
					void* new_newLeafpage = generatePage(new_node);
					ixfileHandle.appendPage(new_newLeafpage);
					nodeMap.insert(make_pair(ixfileHandle.ixAppendPageCounter - 1, new_node));
					((LeafNode*)*new_node)->pageNum = ixfileHandle.ixAppendPageCounter - 1;
					free(new_newLeafpage);

					InternalEntry internal_pair(attrType, ((LeafNode*)*new_node)->leafEntries[0].key, tar_leafNode, new_node);
					//add the entry into to internalnode
					vector<InternalEntry>::iterator internal_iter_vector = (*(InternalNode**)origin_parentNode)->internalEntries.begin();   //find the right position in leafnode
					int isadded1 = 0;
					int added_index = 0;
					for (int i = 0; i < (*(InternalNode**)origin_parentNode)->internalEntries.size(); i++) {
						if (compareKey(internal_pair.key, (*(InternalNode**)origin_parentNode)->internalEntries[i].key)<0) {
							(*(InternalNode**)origin_parentNode)->internalEntries.insert(internal_iter_vector + i, internal_pair);
							isadded1 = 1;
							added_index = i;
							break;
						}
					}
					if (isadded1 == 0) {
						(*(InternalNode**)origin_parentNode)->internalEntries.push_back(internal_pair);
						(*(InternalNode**)origin_parentNode)->internalEntries[(*(InternalNode**)origin_parentNode)->internalEntries.size() - 2].rightChild = internal_pair.leftChild;
					}
					else if (added_index = 0) {
						(*(InternalNode**)origin_parentNode)->internalEntries[1].leftChild = internal_pair.rightChild;
					}
					else {
						(*(InternalNode**)origin_parentNode)->internalEntries[added_index - 1].rightChild = internal_pair.leftChild;
						(*(InternalNode**)origin_parentNode)->internalEntries[added_index + 1].leftChild = internal_pair.rightChild;
					}
					((InternalNode*)*origin_parentNode)->nodeSize += var_length + 2 * sizeof(int) + sizeof(OffsetType);
					// if(((LeafNode*)*origin_parentNode)->nodeSize + var_length +2*sizeof(int)+sizeof(OffsetType)<=PAGE_SIZE){
					// }else{ //split the parentNode

					// }
					//
					if (((InternalNode*)*origin_parentNode)->nodeSize <= PAGE_SIZE) {
						void* new_page = generatePage(tar_leafNode);
						ixfileHandle.writePage((*(InternalNode**)origin_parentNode)->pageNum, new_page);
						free(new_page);
					}
					while (((InternalNode*)*origin_parentNode)->nodeSize > PAGE_SIZE) {
						Node** new_parentNode = new Node*;
						*new_parentNode = new InternalNode();
						((InternalNode*)*new_parentNode)->isLoaded = true;
						//move the origin leafnode the second half records to the new leafnode, and update the nodesize
						for (int i = (*(InternalNode**)origin_parentNode)->internalEntries.size() / 2 + 1; i < (*(InternalNode**)origin_parentNode)->internalEntries.size(); i++) {
							((InternalNode*)*new_parentNode)->internalEntries.push_back((*(InternalNode**)origin_parentNode)->internalEntries[i]);
							if (attrType == TypeVarChar) {
								memcpy(&var_length, (char *)((InternalNode*)*new_parentNode)->internalEntries[i].key, 4);
								var_length += 12;
							}
							else {
								var_length = 12;
							}
							(*(InternalNode**)origin_parentNode)->nodeSize -= var_length + sizeof(OffsetType);
							((InternalNode*)*new_parentNode)->nodeSize += var_length + sizeof(OffsetType);
						}
						//the mid_node key's length
						int mid_index = (*(InternalNode**)origin_parentNode)->internalEntries.size() / 2;
						void* key_mid;
						if (attrType == TypeVarChar) {
							memcpy(&var_length, (char *)((InternalNode*)*new_parentNode)->internalEntries[mid_index].key, 4);
							key_mid = malloc(sizeof(int) + var_length);
							memcpy((char *)key_mid, (char *)((InternalNode*)*new_parentNode)->internalEntries[mid_index].key, sizeof(int) + var_length);
						}
						else {
							var_length = 4;
							key_mid = malloc(sizeof(int));
							memcpy((char *)key_mid, (char *)((InternalNode*)*new_parentNode)->internalEntries[mid_index].key, sizeof(int));
						}
						//delete the second half records from the origin leafnode.
						//for (int i = mid_index; i < (*(InternalNode**)origin_parentNode)->internalEntries.size(); i++) {
							(*(InternalNode**)origin_parentNode)->internalEntries.erase((*(InternalNode**)origin_parentNode)->internalEntries.begin() + mid_index,(*(InternalNode**)origin_parentNode)->internalEntries.end());
						//}
						//update the origin and new node's rightpointer
						((InternalNode*)*new_parentNode)->nodeType = InternalNodeType;
						if (*((*(InternalNode**)origin_parentNode)->parentPointer) == NULL) {
							((InternalNode*)*new_parentNode)->pageNum = ixfileHandle.ixAppendPageCounter + 1;  //assume the new leaf node will append in ixappendpageCounter th
							InternalEntry internal_pair(attrType, key_mid, origin_parentNode, new_parentNode);
							Node** new_grandparentNode = new Node*;
							*new_grandparentNode = new InternalNode();
							((InternalNode*)*new_grandparentNode)->isLoaded = true;
							((InternalNode*)*new_grandparentNode)->internalEntries.push_back(internal_pair);
							((InternalNode*)*new_grandparentNode)->nodeType = InternalNodeType;
							((InternalNode*)*new_grandparentNode)->nodeSize += var_length + 2 * sizeof(int) + sizeof(OffsetType);
							//write the father node to the file
							void* new_page = generatePage(new_grandparentNode);
							ixfileHandle.appendPage(new_page);
							nodeMap.insert(make_pair(ixfileHandle.ixAppendPageCounter - 1, new_grandparentNode));
							((InternalNode*)*new_parentNode)->pageNum = ixfileHandle.ixAppendPageCounter - 1;
							free(new_page);
							//set father-son relatoinship.
							((InternalNode*)*origin_parentNode)->parentPointer = new_grandparentNode;
							((InternalNode*)*new_parentNode)->parentPointer = new_grandparentNode;
							//set meta fields
							ixfileHandle.root = ixfileHandle.ixAppendPageCounter - 1;
							ixfileHandle.writeMetaPage();
							//update the origin leafnode to the file
							void* new_originLeafpage = generatePage(origin_parentNode);
							int originLeafpage = ((InternalNode*)*origin_parentNode)->pageNum;
							ixfileHandle.writePage(originLeafpage, new_originLeafpage);
							free(new_originLeafpage);
							//add the updated leafnode to the file
							void* new_newLeafpage = generatePage(new_parentNode);
							ixfileHandle.appendPage(new_newLeafpage);
							nodeMap.insert(make_pair(ixfileHandle.ixAppendPageCounter - 1, new_parentNode));
							((InternalNode*)*new_parentNode)->pageNum = ixfileHandle.ixAppendPageCounter - 1;
							free(new_newLeafpage);
							break;
						}
						else {  // it have the parentNode
							Node** origin_grandparentNode = new Node*;
							PageNum origin_grandparentNode_pagenum = ((InternalNode*)*(((InternalNode*)*origin_parentNode)->parentPointer))->pageNum;
							iter = nodeMap.find(origin_grandparentNode_pagenum);
							if (iter != nodeMap.end()) {
								origin_grandparentNode = iter->second;
							}
							else {
								cout << "not found" << endl;
							}
							//update the origin leafnode to the file

							void* new_originLeafpage = generatePage(origin_parentNode);
							int originLeafpage = ((InternalNode*)*origin_parentNode)->pageNum;
							ixfileHandle.writePage(originLeafpage, new_originLeafpage);
							free(new_originLeafpage);
							//add the updated leafnode to the file
							((InternalNode*)*new_parentNode)->parentPointer = origin_grandparentNode;
							void* new_newLeafpage = generatePage(new_parentNode);
							ixfileHandle.appendPage(new_newLeafpage);
							nodeMap.insert(make_pair(ixfileHandle.ixAppendPageCounter - 1, new_parentNode));
							((InternalNode*)*new_parentNode)->pageNum = ixfileHandle.ixAppendPageCounter - 1;
							free(new_newLeafpage);

							InternalEntry internal_pair(attrType, key_mid, origin_parentNode, new_parentNode);
							//add the entry into to internalnode
							vector<InternalEntry>::iterator internal_iter_vector = (*(InternalNode**)origin_grandparentNode)->internalEntries.begin();   //find the right position in leafnode
							int isadded1 = 0;
							int added_index = 0;
							for (int i = 0; i < (*(InternalNode**)origin_grandparentNode)->internalEntries.size(); i++) {

								if (compareKey(internal_pair.key, (*(InternalNode**)origin_grandparentNode)->internalEntries[i].key)<0) {

									(*(InternalNode**)origin_grandparentNode)->internalEntries.insert(internal_iter_vector + i, internal_pair);
									isadded1 = 1;
									added_index = i;
									break;
								}
							}
							if (isadded1 == 0) {
								(*(InternalNode**)origin_grandparentNode)->internalEntries.push_back(internal_pair);
								(*(InternalNode**)origin_grandparentNode)->internalEntries[(*(InternalNode**)origin_grandparentNode)->internalEntries.size() - 2].rightChild = internal_pair.leftChild;
							}
							else if (added_index = 0) {
								(*(InternalNode**)origin_grandparentNode)->internalEntries[1].leftChild = internal_pair.rightChild;
							}
							else {
								(*(InternalNode**)origin_grandparentNode)->internalEntries[added_index - 1].rightChild = internal_pair.leftChild;
								(*(InternalNode**)origin_grandparentNode)->internalEntries[added_index + 1].leftChild = internal_pair.rightChild;
							}
							((InternalNode*)*origin_grandparentNode)->nodeSize += var_length + 2 * sizeof(int) + sizeof(OffsetType);
							if (((InternalNode*)*origin_grandparentNode)->nodeSize <= PAGE_SIZE) {
								void* new_page = generatePage(origin_grandparentNode);
								ixfileHandle.writePage((*(InternalNode**)origin_grandparentNode)->pageNum, new_page);
								free(new_page);
								break;
							}
							origin_parentNode = origin_grandparentNode;
						}
						//update the origin_parentNode, internal_pair,var_legnth,
					}
				}
			}
		}
		return 0;
}

Node** BTree::findLeafnode(IXFileHandle &ixfileHandle, const LeafEntry &pair, Node** cur_node) {
	while ((*cur_node)->nodeType != LeafNodeType) {
		int size = (*(InternalNode**)cur_node)->internalEntries.size();
		if (attrType == TypeInt) {                    // key is int
			int key;
			memcpy(&key, (char *)pair.key, sizeof(int));
			for (int i = 0; i <= size; i++) {
				if (i == size) {
					cur_node = (*(InternalNode**)cur_node)->internalEntries[size - 1].rightChild;
					if ((*(InternalNode**)cur_node)->isLoaded == false) {
						void* data = malloc(PAGE_SIZE);
						PageNum cur_pageNum = (*(InternalNode**)cur_node)->pageNum;
						ixfileHandle.readPage(cur_pageNum, data);
						cur_node = generateNode((char *)data);
						nodeMap.insert(make_pair(cur_pageNum, cur_node));
					}
					break;
				}
				else {
					int sou_key;
					memcpy(&sou_key, (*(InternalNode**)cur_node)->internalEntries[i].key, sizeof(int));
					if (key < sou_key) {
						cur_node = (*(InternalNode**)cur_node)->internalEntries[i].leftChild;
						if ((*(InternalNode**)cur_node)->isLoaded == false) {
							void* data = malloc(PAGE_SIZE);
							PageNum cur_pageNum = (*(InternalNode**)cur_node)->pageNum;
							ixfileHandle.readPage(cur_pageNum, data);
							cur_node = generateNode((char *)data);
							nodeMap.insert(make_pair(cur_pageNum, cur_node));
						}
						break;
					}
				}
			}
		}
		else if (attrType == TypeReal) {
			float key;
			memcpy(&key, (char *)pair.key, sizeof(float));
			for (int i = 0; i <= size; i++) {
				if (i == size) {
					cur_node = (*(InternalNode**)cur_node)->internalEntries[size - 1].rightChild;
					if ((*(InternalNode**)cur_node)->isLoaded == false) {
						void* data = malloc(PAGE_SIZE);
						PageNum cur_pageNum = (*(InternalNode**)cur_node)->pageNum;
						ixfileHandle.readPage(cur_pageNum, data);
						cur_node = generateNode((char *)data);
						nodeMap.insert(make_pair(cur_pageNum, cur_node));
					}
					break;
				}
				else {
					int sou_key;
					memcpy(&sou_key, (*(InternalNode**)cur_node)->internalEntries[i].key, sizeof(int));
					if (key < sou_key) {
						cur_node = (*(InternalNode**)cur_node)->internalEntries[i].leftChild;
						if ((*(InternalNode**)cur_node)->isLoaded == false) {
							void* data = malloc(PAGE_SIZE);
							PageNum cur_pageNum = (*(InternalNode**)cur_node)->pageNum;
							ixfileHandle.readPage(cur_pageNum, data);
							cur_node = generateNode((char *)data);
							nodeMap.insert(make_pair(cur_pageNum, cur_node));
						}
						break;
					}
				}
			}
		}
		else if (attrType == TypeVarChar) {
			for (int i = 0; i <= size; i++) {
				if (i == size) {
					cur_node = (*(InternalNode**)cur_node)->internalEntries[size - 1].rightChild;
					if ((*(InternalNode**)cur_node)->isLoaded == false) {
						void* data = malloc(PAGE_SIZE);
						PageNum cur_pageNum = (*(InternalNode**)cur_node)->pageNum;
						ixfileHandle.readPage(cur_pageNum, data);
						cur_node = generateNode((char *)data);
						nodeMap.insert(make_pair(cur_pageNum, cur_node));
					}
					break;
				}
				else {
					if (compareKey(pair.key, (*(InternalNode**)cur_node)->internalEntries[i].key) < 0) {
						cur_node = (*(InternalNode**)cur_node)->internalEntries[i].leftChild;
						if ((*(InternalNode**)cur_node)->isLoaded == false) {
							void* data = malloc(PAGE_SIZE);
							PageNum cur_pageNum = (*(InternalNode**)cur_node)->pageNum;
							ixfileHandle.readPage(cur_pageNum, data);
							cur_node = generateNode((char *)data);
							nodeMap.insert(make_pair(cur_pageNum, cur_node));
						}
						break;
					}
				}
			}
		}
	}
	return cur_node;
}

int BTree::compareKey(void* v1, void* v2)
{
	if (this->attrType == AttrType::TypeInt)
	{
		return *(int*)v1 - *(int*)v2;
	}
	else if (this->attrType == AttrType::TypeReal)
	{
		return *(float*)v1 - *(float*)v2;
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
	if (this->attrType == AttrType::TypeInt)
	{
		int k1 = *(int*)pair1.key;
		int k2 = *(int*)pair2.key;
		if (k1 != k2)
			return k1 - k2;
	}
	else if (this->attrType == AttrType::TypeReal)
	{
		int k1 = *(float*)pair1.key;
		int k2 = *(float*)pair2.key;
		if (k1 != k2)
			return k1 - k2;
	}
	else if (this->attrType == AttrType::TypeVarChar)
	{
		int strLength1 = *(int*)pair1.key;
		int strLength2 = *(int*)pair2.key;
		string s1((char*)pair1.key + sizeof(int), strLength1);
		string s2((char*)pair2.key + sizeof(int), strLength2);
		int keyCompareResult = s1.compare(s2);
		if (keyCompareResult != 0)
			return keyCompareResult;
	}
	if (pair1.rid.pageNum - pair2.rid.pageNum != 0)
		return pair1.rid.pageNum - pair2.rid.pageNum;
	return pair1.rid.slotNum - pair2.rid.slotNum;
}

RC BTree::loadNode(IXFileHandle &ixfileHandle, Node** &target)
{
	if (target == NULL || *target == NULL)
	{
#ifdef DEBUG
		cerr << "The node is NULL when loading the node" << endl;
#endif
		return -1;
	}

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
	if (*node == NULL)
	{
#ifdef DEBUG
		cerr << "The node is NULL when removing entry from the node" << endl;
#endif
		return -1;
	}

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
				(*node)->nodeSize -= decreaseSize;
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
				(*node)->nodeSize -= decreaseSize;
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
	//Pretend that we read the root node page
	++ixfileHandle.ixReadPageCounter;

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
			if (compareKey(((InternalNode*)*currentNode)->internalEntries[i].key, pair.key) <= 0)
				++i;
			else
				break;
		}
		if (((InternalNode*)*currentNode)->internalEntries[i].leftChild)
		{
			if (*((InternalNode*)*currentNode)->internalEntries[i].leftChild)
			{
				if (!(*((InternalNode*)*currentNode)->internalEntries[i].leftChild)->isLoaded)
				{
					//If the node was not loaded before, we need to load it from page file
					if (loadNode(ixfileHandle, ((InternalNode*)*currentNode)->internalEntries[i].leftChild) == -1)
					{
#ifdef DEBUG
						cerr << "Cannot load the left child node when finding the leaf" << endl;
#endif
						return -1;
					}
				}
				else
				{
					//Pretend that we read the leftchild node page
					++ixfileHandle.ixReadPageCounter;
				}
				currentNode = ((InternalNode*)*currentNode)->internalEntries[i].leftChild;
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
	return 0;
}

//Finds and returns the record to which a key refers.
RC BTree::findRecord(IXFileHandle &ixfileHandle, const LeafEntry &pair, vector<LeafEntry>::iterator &result)
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
	result = (*leaf)->leafEntries.begin();
	for (; result != (*leaf)->leafEntries.end(); result++)
		if (compareEntry(*result, pair) == 0)
			break;
	if (result == (*leaf)->leafEntries.end())
	{
#ifdef DEBUG
		cerr << "Cannot find the record in the leaf node when finding record, rid = " << pair.rid.pageNum << ", " << pair.rid.slotNum << endl;
#endif
		return -1;
	}
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
	if (*node == NULL)
	{
#ifdef DEBUG
		cerr << "The node is NULL when getting neighbor index of the node" << endl;
#endif
		return -1;
	}

	size_t internalEntriesSize = ((InternalNode*)((*node)->parentPointer))->internalEntries.size();
	for (size_t i = 0; i < internalEntriesSize; i++)
	{
		if (*((InternalNode*)((*node)->parentPointer))->internalEntries[i].leftChild == *node)
		{
			result = i - 1;
			return 0;
		}
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

OffsetType BTree::getKeySize(const void* key)
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

RC BTree::getNodesMergeSize(Node** node1, Node** node2, OffsetType sizeOfParentKey, OffsetType &result)
{
	if (*node1 == NULL)
	{
#ifdef DEBUG
		cerr << "The node1 is NULL when getting nodes merge size" << endl;
#endif
		return -1;
	}
	if (*node2 == NULL)
	{
#ifdef DEBUG
		cerr << "The node2 is NULL when getting nodes merge size" << endl;
#endif
		return -1;
	}

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

RC BTree::mergeNodes(IXFileHandle &ixfileHandle, Node** node, Node** neighbor, int neighborIndex, int keyIndex, OffsetType mergedNodeSize)
{
	if (*node == NULL)
	{
#ifdef DEBUG
		cerr << "The node is NULL when merging nodes" << endl;
#endif
		return -1;
	}
	if (*neighbor == NULL)
	{
#ifdef DEBUG
		cerr << "The neighbor node is NULL when merging nodes" << endl;
#endif
		return -1;
	}

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
		((LeafNode*)*neighbor)->rightPointer = ((LeafNode*)*node)->rightPointer;

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
	*node = NULL;
	return 0;
}

RC BTree::redistributeNodes(Node** node, Node** neighbor, int neighborIndex, int keyIndex, OffsetType keySize)
{
	if (*node == NULL)
	{
#ifdef DEBUG
		cerr << "The node is NULL when redistributing nodes" << endl;
#endif
		return -1;
	}
	if (*neighbor == NULL)
	{
#ifdef DEBUG
		cerr << "The neighbor node is NULL when merging nodes" << endl;
#endif
		return -1;
	}

	//If neighbor node only has one child, we cannot and do not need to redistribute
	if ((*node)->nodeType == InternalNodeType && (*neighbor)->nodeType == InternalNodeType)
	{
		if (((InternalNode*)*neighbor)->internalEntries.size() == 1)
			return 0;
	}
	else if ((*node)->nodeType == LeafNodeType && (*neighbor)->nodeType == LeafNodeType)
	{
		if (((LeafNode*)*neighbor)->leafEntries.size() == 1)
			return 0;
	}
	else
	{
#ifdef DEBUG
		cerr << "The type of node and that of neighbor are different when redistributing nodes, and neighbor node only has one child" << endl;
#endif
		return -1;
	}

	if (neighborIndex != -1)
	{
		//Case: node has a neighbor to the left. 
		//Pull the neighbor's last key-pointer pair over from the neighbor's right end to node's left end.
		if ((*node)->nodeType == InternalNodeType && (*neighbor)->nodeType == InternalNodeType)
		{
			//If the entry that we decide to borrow from neighbor is too big, we cannot and do not need to redistribute either
			OffsetType sizeOfKeyInNeighbor = getKeySize(((InternalNode*)*(*node)->parentPointer)->internalEntries[keyIndex].key);
			OffsetType finalNodeSize = (*node)->nodeSize + sizeOfKeyInNeighbor + sizeof(PageNum) + sizeof(OffsetType); //key + leftpointer + slotOffset
			if (finalNodeSize > PAGE_SIZE)
				return 0;

			//Copy the rightmost key in the neighbor node and insert it in the front of node
			//The value of the borrowed key is the middle key value
			InternalEntry borrowedEntry(this->attrType, ((InternalNode*)*(*node)->parentPointer)->internalEntries[keyIndex].key);
			borrowedEntry.leftChild = ((InternalNode*)*neighbor)->internalEntries.back().rightChild;
			(*borrowedEntry.leftChild)->parentPointer = node;
			borrowedEntry.rightChild = ((InternalNode*)*node)->internalEntries[0].leftChild;

			//If the entry that we decide to update in parent node is too big, we cannot and do not need to redistribute either
			OffsetType sizeOfKeyInParent = getKeySize(((InternalNode*)*neighbor)->internalEntries.back().key);
			OffsetType finalParentSize = ((InternalNode*)*(*node)->parentPointer)->nodeSize + sizeOfKeyInParent - getKeySize(((InternalNode*)*(*node)->parentPointer)->internalEntries[keyIndex].key);
			if (finalParentSize > PAGE_SIZE)
				return 0;

			//Update the middle key value in the parent node into the value of rightmost key in the neighbor node
			InternalEntry parentEntry(this->attrType, ((InternalNode*)*neighbor)->internalEntries.back().key);
			parentEntry.leftChild = ((InternalNode*)*(*node)->parentPointer)->internalEntries[keyIndex].leftChild;
			parentEntry.rightChild = ((InternalNode*)*(*node)->parentPointer)->internalEntries[keyIndex].rightChild;

			//Insert the borrowed entry, update parent node, and delete the entry in the neighbor node
			((InternalNode*)*node)->internalEntries.insert(((InternalNode*)*node)->internalEntries.begin(), borrowedEntry);
			((InternalNode*)*(*node)->parentPointer)->internalEntries[keyIndex] = parentEntry;
			((InternalNode*)*neighbor)->internalEntries.erase(((InternalNode*)*neighbor)->internalEntries.end() - 1);
		}
		else if ((*node)->nodeType == LeafNodeType && (*neighbor)->nodeType == LeafNodeType)
		{
			//Copy the rightmost key in the neighbor node and insert it in the front of node
			LeafEntry borrowedEntry(this->attrType, ((LeafNode*)*neighbor)->leafEntries.back().key, ((LeafNode*)*neighbor)->leafEntries.back().rid);

			//If the entry that we decide to borrow from neighbor is too big, we cannot and do not need to redistribute either
			OffsetType sizeOfEntryInNeighbor = getEntrySize(LeafNodeType, borrowedEntry, false);
			OffsetType finalNodeSize = (*node)->nodeSize + sizeOfEntryInNeighbor + sizeof(OffsetType); //key + leftpointer + slotOffset
			if (finalNodeSize > PAGE_SIZE)
				return 0;
			
			//If the entry that we decide to update in parent node is too big, we cannot and do not need to redistribute either
			OffsetType sizeOfKeyInParent = getKeySize(((LeafNode*)*node)->leafEntries[0].key);
			OffsetType finalParentSize = ((InternalNode*)*(*node)->parentPointer)->nodeSize + sizeOfKeyInParent - getKeySize(((InternalNode*)*(*node)->parentPointer)->internalEntries[keyIndex].key);
			if (finalParentSize > PAGE_SIZE)
				return 0;

			//Update the middle key value in the parent node into the value of rightmost key in the neighbor node
			InternalEntry parentEntry(this->attrType, ((LeafNode*)*node)->leafEntries[0].key);
			parentEntry.leftChild = ((InternalNode*)*(*node)->parentPointer)->internalEntries[keyIndex].leftChild;
			parentEntry.rightChild = ((InternalNode*)*(*node)->parentPointer)->internalEntries[keyIndex].rightChild;

			//Insert the borrowed entry, update parent node, and delete the entry in the neighbor node
			((LeafNode*)*node)->leafEntries.insert(((LeafNode*)*node)->leafEntries.begin(), borrowedEntry);
			((InternalNode*)*(*node)->parentPointer)->internalEntries[keyIndex] = parentEntry;
			((LeafNode*)*neighbor)->leafEntries.erase(((LeafNode*)*neighbor)->leafEntries.end() - 1);
		}
		else
		{
#ifdef DEBUG
			cerr << "The type of node and that of neighbor are different when redistributing nodes" << endl;
#endif
			return -1;
		}
	}
	else
	{
		//Case: node is the leftmost child.
		//Take a key-pointer pair from the neighbor to the right.
		//Move the neighbor's leftmost key-pointer pair to node's rightmost position.
		if ((*node)->nodeType == InternalNodeType && (*neighbor)->nodeType == InternalNodeType)
		{
			//If the entry that we decide to borrow from neighbor is too big, we cannot and do not need to redistribute either
			OffsetType sizeOfKeyInNeighbor = getKeySize(((InternalNode*)*(*node)->parentPointer)->internalEntries[keyIndex].key);
			OffsetType finalNodeSize = (*node)->nodeSize + sizeOfKeyInNeighbor + sizeof(PageNum) + sizeof(OffsetType); //key + leftpointer + slotOffset
			if (finalNodeSize > PAGE_SIZE)
				return 0;

			//Copy the rightmost key in the neighbor node and insert it in the front of node
			//The value of the borrowed key is the middle key value
			InternalEntry borrowedEntry(this->attrType, ((InternalNode*)*(*node)->parentPointer)->internalEntries[keyIndex].key);
			borrowedEntry.leftChild = ((InternalNode*)*node)->internalEntries.back().rightChild;
			(*borrowedEntry.rightChild)->parentPointer = node;
			borrowedEntry.rightChild = ((InternalNode*)*neighbor)->internalEntries[0].leftChild;

			//If the entry that we decide to update in parent node is too big, we cannot and do not need to redistribute either
			OffsetType sizeOfKeyInParent = getKeySize(((InternalNode*)*neighbor)->internalEntries[0].key);
			OffsetType finalParentSize = ((InternalNode*)*(*node)->parentPointer)->nodeSize + sizeOfKeyInParent - getKeySize(((InternalNode*)*(*node)->parentPointer)->internalEntries[keyIndex].key);
			if (finalParentSize > PAGE_SIZE)
				return 0;

			//Update the middle key value in the parent node into the value of leftmost key in the neighbor node
			InternalEntry parentEntry(this->attrType, ((InternalNode*)*neighbor)->internalEntries[0].key);
			parentEntry.leftChild = ((InternalNode*)*(*node)->parentPointer)->internalEntries[keyIndex].leftChild;
			parentEntry.rightChild = ((InternalNode*)*(*node)->parentPointer)->internalEntries[keyIndex].rightChild;

			//Insert the borrowed entry, update parent node, and delete the entry in the neighbor node
			((InternalNode*)*node)->internalEntries.push_back(borrowedEntry);
			((InternalNode*)*(*node)->parentPointer)->internalEntries[keyIndex] = parentEntry;
			((InternalNode*)*neighbor)->internalEntries.erase(((InternalNode*)*neighbor)->internalEntries.begin());
		}
		else if ((*node)->nodeType == LeafNodeType && (*neighbor)->nodeType == LeafNodeType)
		{
			//Copy the rightmost key in the neighbor node and insert it in the front of node
			LeafEntry borrowedEntry(this->attrType, ((LeafNode*)*neighbor)->leafEntries[0].key, ((LeafNode*)*neighbor)->leafEntries.back().rid);
			
			//If the entry that we decide to borrow from neighbor is too big, we cannot and do not need to redistribute either
			OffsetType sizeOfEntryInNeighbor = getEntrySize(LeafNodeType, borrowedEntry, false);
			OffsetType finalNodeSize = (*node)->nodeSize + sizeOfEntryInNeighbor + sizeof(OffsetType); //key + leftpointer + slotOffset
			if (finalNodeSize > PAGE_SIZE)
				return 0;

			//If the entry that we decide to update in parent node is too big, we cannot and do not need to redistribute either
			OffsetType sizeOfKeyInParent = getKeySize(((LeafNode*)*node)->leafEntries[0].key);
			OffsetType finalParentSize = ((InternalNode*)*(*node)->parentPointer)->nodeSize + sizeOfKeyInParent - getKeySize(((InternalNode*)*(*node)->parentPointer)->internalEntries[keyIndex].key);
			if (finalParentSize > PAGE_SIZE)
				return 0;

			//Update the middle key value in the parent node into the value of leftmost key in the neighbor node
			InternalEntry parentEntry(this->attrType, ((LeafNode*)*neighbor)->leafEntries[0].key);
			parentEntry.leftChild = ((InternalNode*)*(*node)->parentPointer)->internalEntries[keyIndex].leftChild;
			parentEntry.rightChild = ((InternalNode*)*(*node)->parentPointer)->internalEntries[keyIndex].rightChild;

			//Insert the borrowed entry, update parent node, and delete the entry in the neighbor node
			((LeafNode*)*node)->leafEntries.push_back(borrowedEntry);
			((InternalNode*)*(*node)->parentPointer)->internalEntries[keyIndex] = parentEntry;
			((LeafNode*)*neighbor)->leafEntries.erase(((LeafNode*)*neighbor)->leafEntries.begin());
		}
		else
		{
#ifdef DEBUG
			cerr << "The type of node and that of neighbor are different when redistributing nodes, and the node is the leftmost node" << endl;
#endif
			return -1;
		}
	}
	//Update node, neighbor node and parent node
	(*node)->isDirty = true;
	if (refreshNodeSize(node) == -1)
	{
#ifdef DEBUG
		cerr << "Cannot refresh the node when redistributing nodes" << endl;
#endif
		return -1;
	}
	(*neighbor)->isDirty = true;
	if (refreshNodeSize(neighbor) == -1)
	{
#ifdef DEBUG
		cerr << "Cannot refresh the neighbor node when redistributing nodes" << endl;
#endif
		return -1;
	}
	(*(*node)->parentPointer)->isDirty = true;
	if (refreshNodeSize((*node)->parentPointer) == -1)
	{
#ifdef DEBUG
		cerr << "Cannot refresh the parent node when redistributing nodes" << endl;
#endif
		return -1;
	}
	return 0;
}

RC BTree::refreshNodeSize(Node** node)
{
	if (*node == NULL)
	{
#ifdef DEBUG
		cerr << "The node is NULL when refreshing node size" << endl;
#endif
		return -1;
	}

	if ((*node)->nodeType = InternalNodeType)
	{
		int size = sizeof(MarkType) + sizeof(OffsetType) + sizeof(PageNum); //type + usedspace + parent pointer
		for (auto &i : ((InternalNode*)*node)->internalEntries)
		{
			size += sizeof(PageNum); //left page pointer
			size += getKeySize(i.key); //key
		}
		size += sizeof(PageNum); //rightmost page pointer
		size += ((InternalNode*)*node)->internalEntries.size() * sizeof(OffsetType); //slotOffsets
		size += sizeof(OffsetType); //slotCount
		(*node)->nodeSize = size;
	}
	else if ((*node)->nodeType = LeafNodeType)
	{
		int size = sizeof(MarkType) + sizeof(OffsetType) + sizeof(PageNum) * 3; //type + usedspace + parent pointer + right pointer + overflow pointer
		for (auto &i : ((LeafNode*)*node)->leafEntries)
		{
			size += sizeof(PageNum); //left page pointer
			size += getKeySize(i.key); //key
		}
		size += sizeof(PageNum); //rightmost page pointer
		size += ((LeafNode*)*node)->leafEntries.size() * sizeof(OffsetType); //slotOffsets
		size += sizeof(OffsetType); //slotCount
		(*node)->nodeSize = size;
	}
	return 0;
}

RC BTree::writeNodesBack(IXFileHandle &ixfileHandle)
{
	for (auto &i : this->nodeMap)
	{
		if ((*i.second)->isDirty)
		{
			if (*i.second == NULL)
			{
#ifdef DEBUG
				cerr << "The node is NULL when writing back nodes" << endl;
#endif
				return -1;
			}
			char* pageData = generatePage(i.second);
			if (ixfileHandle.writePage(i.first, pageData) == -1)
			{
#ifdef DEBUG
				cerr << "Cannot write the node back, pageNum = " << (*i.second)->pageNum << endl;
#endif
				return -1;
			}
			free(pageData);
		}
	}
	return 0;
}

//Deletes an entry from the B+ tree
RC BTree::doDelete(IXFileHandle &ixfileHandle, Node** node, const LeafEntry &pair)
{
	if (*node == NULL)
	{
#ifdef DEBUG
		cerr << "The node is NULL when doing delete" << endl;
#endif
		return -1;
	}

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
	//Pretend that we read the neighbor node page
	++ixfileHandle.ixReadPageCounter;
	if (getNeighborIndex(node, neighborIndex) == -1)
	{
#ifdef DEBUG
		cerr << "Cannot get the neighbor node index when deleting entry, rid = " << pair.rid.pageNum << ", " << pair.rid.slotNum << endl;
#endif
		return -1;
	}
	//The index of key which between the pointer to the neighbor and the pointer to the node
	int keyIndex = neighborIndex == -1 ? 0 : neighborIndex;
	//Pretend that we read the neighbor node page
	++ixfileHandle.ixReadPageCounter;
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
		return mergeNodes(ixfileHandle, node, neighbor, neighborIndex, keyIndex, sizeOfMergeNodes);
	else
		return redistributeNodes(node, neighbor, neighborIndex, keyIndex, keySize);
}

//Master deletion function
RC BTree::deleteEntry(IXFileHandle &ixfileHandle, const LeafEntry &pair)
{
	vector<LeafEntry>::iterator leafRecord;
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
	if (leaf != NULL && leafRecord != (*leaf)->leafEntries.end())
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
	if (writeNodesBack(ixfileHandle) == -1)
	{
#ifdef DEBUG
		cerr << "Cannot write the node back when deleting entry, rid = " << pair.rid.pageNum << ", " << pair.rid.slotNum << endl;
#endif
		return -1;
	}
	return 0;
}

char* BTree::generatePage(Node** node)
{
	if (*node == NULL)
	{
#ifdef DEBUG
		cerr << "The node is NULL when generating page" << endl;
#endif
		return NULL;
	}

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
		//Store the pageNum of right pointer
		if (((const LeafNode*)*node)->rightPointer && *((const LeafNode*)*node)->rightPointer)
		{
			PageNum rightPageNum = (*((const LeafNode*)*node)->rightPointer)->pageNum;
			memcpy(pageData + offset, &rightPageNum, sizeof(PageNum));
			offset += sizeof(PageNum);
		}
		else
		{
			PageNum rightPageNum = NULLNODE;
			memcpy(pageData + offset, &rightPageNum, sizeof(PageNum));
			offset += sizeof(PageNum);
		}
		//Store the pageNum of overflow pointer
		if (((const LeafNode*)*node)->overflowPointer && *((const LeafNode*)*node)->overflowPointer)
		{
			PageNum overflowPageNum = (*((const LeafNode*)*node)->overflowPointer)->pageNum;
			memcpy(pageData + offset, &overflowPageNum, sizeof(PageNum));
			offset += sizeof(PageNum);
		}
		else
		{
			PageNum overflowPageNum = NULLNODE;
			memcpy(pageData + offset, &overflowPageNum, sizeof(PageNum));
			offset += sizeof(PageNum);
		}
		
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
	else
	{
#ifdef DEBUG
		cerr << "The node type is UNKNOWN when generating page" << endl;
#endif
		return NULL;
	}
	return pageData;
}
Node** BTree::generateNode(char* data)
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
			(*result)->parentPointer = NULL;

		OffsetType slotCount;
		memcpy(&slotCount, data + PAGE_SIZE - sizeof(OffsetType), sizeof(OffsetType));
		for (OffsetType i = 0; i < slotCount; i++)
		{
			OffsetType slotOffset;
			memcpy(&slotOffset, data + PAGE_SIZE - sizeof(OffsetType) * (i + 2), sizeof(OffsetType));
			InternalEntry entry(this->attrType, data);
			OffsetType keyLength = 0;
			if (this->attrType = AttrType::TypeInt)
			{
				keyLength = sizeof(int);
			}
			else if (this->attrType = AttrType::TypeReal)
			{
				keyLength = sizeof(float);
			}
			else if (this->attrType = AttrType::TypeVarChar)
			{
				int strLength = *(int*)(data + slotOffset);
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
			search = this->nodeMap.find(rightChildPageNum);
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
				keyLength = sizeof(int);
			}
			else if (this->attrType = AttrType::TypeReal)
			{
				keyLength = sizeof(float);
			}
			else if (this->attrType = AttrType::TypeVarChar)
			{
				int strLength = *(int*)(data + slotOffset);
				keyLength = sizeof(int) + strLength;
			}
			entry.size = keyLength;
			entry.key = malloc(keyLength);
			memcpy(entry.key, data + slotOffset, keyLength);
			memcpy(&entry.rid.pageNum, data + slotOffset + keyLength, sizeof(PageNum));
			memcpy(&entry.rid.slotNum, data + slotOffset + keyLength + sizeof(PageNum), sizeof(unsigned));

			((LeafNode*)*result)->leafEntries.push_back(entry);
		}
	}
	return result;
}

