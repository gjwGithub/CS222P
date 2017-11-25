
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
}

IndexManager::~IndexManager()
{
	if (_index_manager)
		delete _index_manager;
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
	if (ixfileHandle.tree)
	{
		if (ixfileHandle.tree->root && (*ixfileHandle.tree->root))
			ixfileHandle.root = (*ixfileHandle.tree->root)->pageNum;
		else
			ixfileHandle.root = NULLNODE;
		if (ixfileHandle.tree->smallestLeaf && (*ixfileHandle.tree->smallestLeaf))
			ixfileHandle.smallestLeaf = (*ixfileHandle.tree->smallestLeaf)->pageNum;
		else
			ixfileHandle.smallestLeaf = NULLNODE;
	}
	return 0;
}

RC IndexManager::closeFile(IXFileHandle &ixfileHandle)
{
	if (ixfileHandle.tree && ixfileHandle.tree->writeNodesBack(ixfileHandle) == -1)
	{
#ifdef DEBUG
		cerr << "Cannot write the nodes back when closing file" << endl;
#endif
		return -1;
	}
	refreshMetaData(ixfileHandle);
	if (ixfileHandle.writeMetaPage() == -1)
		return -1;
	if (PagedFileManager::instance()->closeFile(ixfileHandle.handle) == -1)
		return -1;
	if (ixfileHandle.tree)
		delete ixfileHandle.tree;
	return 0;
}

RC IndexManager::insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
	//LeafEntry leafEntry(attribute.type, key, rid);
	//if (ixfileHandle.tree == NULL) {
	//	ixfileHandle.tree = new BTree();
	//	ixfileHandle.tree->attrType = attribute.type;
	//}
	//if (ixfileHandle.root == -1) {
	//	ixfileHandle.tree->root = NULL;
	//}
	//RC rel = ixfileHandle.tree->insertEntry(ixfileHandle, leafEntry);
	//return rel;

	LeafEntry leafEntry(attribute.type, key, rid);
	if (ixfileHandle.tree)
	{
		RC status = ixfileHandle.tree->insertEntry(ixfileHandle, leafEntry);
		return status;
	}
	else
	{
		ixfileHandle.tree = new BTree();
		ixfileHandle.tree->attrType = attribute.type;

		if (ixfileHandle.root == NULLNODE)
		{
			ixfileHandle.tree->root = NULL;
		}
		else
		{
			char *rootPage = (char*)malloc(PAGE_SIZE);
			ixfileHandle.readPage(ixfileHandle.root, rootPage);
			ixfileHandle.tree->root = ixfileHandle.tree->generateNode(rootPage);
			(*ixfileHandle.tree->root)->pageNum = ixfileHandle.root;
			free(rootPage);
			ixfileHandle.tree->nodeMap.insert(make_pair(ixfileHandle.root, ixfileHandle.tree->root));
		}

		if (ixfileHandle.root == ixfileHandle.smallestLeaf)
		{
			ixfileHandle.tree->smallestLeaf = ixfileHandle.tree->root;
		}
		else
		{
			char *smallestLeafPage = (char*)malloc(PAGE_SIZE);
			ixfileHandle.readPage(ixfileHandle.smallestLeaf, smallestLeafPage);
			ixfileHandle.tree->smallestLeaf = ixfileHandle.tree->generateNode(smallestLeafPage);
			(*ixfileHandle.tree->smallestLeaf)->pageNum = ixfileHandle.smallestLeaf;
			free(smallestLeafPage);
			ixfileHandle.tree->nodeMap.insert(make_pair(ixfileHandle.smallestLeaf, ixfileHandle.tree->smallestLeaf));
		}

		RC status = ixfileHandle.tree->insertEntry(ixfileHandle, leafEntry);
		return status;
	}
	return 0;
}

RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
	LeafEntry leafEntry(attribute.type, key, rid);
	if (ixfileHandle.tree)
	{
		RC status = ixfileHandle.tree->deleteEntry(ixfileHandle, leafEntry);
		free(leafEntry.key);
		return status;
	}
	else
	{
		ixfileHandle.tree = new BTree();
		ixfileHandle.tree->attrType = attribute.type;

		if (ixfileHandle.root == NULLNODE)
		{
#ifdef DEBUG
			cerr << "Cannot read the root node when deleting entry" << endl;
#endif
			free(leafEntry.key);
			return -1;
		}
		char *rootPage = (char*)malloc(PAGE_SIZE);
		ixfileHandle.readPage(ixfileHandle.root, rootPage);
		ixfileHandle.tree->root = ixfileHandle.tree->generateNode(rootPage);
		(*ixfileHandle.tree->root)->pageNum = ixfileHandle.root;
		free(rootPage);
		ixfileHandle.tree->nodeMap.insert(make_pair(ixfileHandle.root, ixfileHandle.tree->root));

		if (ixfileHandle.root == ixfileHandle.smallestLeaf)
		{
			ixfileHandle.tree->smallestLeaf = ixfileHandle.tree->root;
		}
		else
		{
			char *smallestLeafPage = (char*)malloc(PAGE_SIZE);
			ixfileHandle.readPage(ixfileHandle.smallestLeaf, smallestLeafPage);
			ixfileHandle.tree->smallestLeaf = ixfileHandle.tree->generateNode(smallestLeafPage);
			(*ixfileHandle.tree->smallestLeaf)->pageNum = ixfileHandle.smallestLeaf;
			free(smallestLeafPage);
			ixfileHandle.tree->nodeMap.insert(make_pair(ixfileHandle.smallestLeaf, ixfileHandle.tree->smallestLeaf));
		}

		RC status = ixfileHandle.tree->deleteEntry(ixfileHandle, leafEntry);
		free(leafEntry.key);
		return status;
	}
	free(leafEntry.key);
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

int IndexManager::compareKey(AttrType attrType, const void* v1, const void* v2) const
{
	if (attrType == AttrType::TypeInt)
	{
		return *(int*)v1 - *(int*)v2;
	}
	else if (attrType == AttrType::TypeReal)
	{
		return floor(*(float*)v1 - *(float*)v2);
	}
	else if (attrType == AttrType::TypeVarChar)
	{
		int strLength1 = *(int*)v1;
		int strLength2 = *(int*)v2;
		string s1((char*)v1 + sizeof(int), strLength1);
		string s2((char*)v2 + sizeof(int), strLength2);
		return s1.compare(s2);
	}
	return 0;
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

	if (ixfileHandle.handle.getFile())
	{
		if (ixfileHandle.tree == NULL)
		{
			ixfileHandle.tree = new BTree();
			ixfileHandle.tree->attrType = attribute.type;

			char *rootPage = (char*)malloc(PAGE_SIZE);
			ixfileHandle.readPage(ixfileHandle.root, rootPage);
			ixfileHandle.tree->root = ixfileHandle.tree->generateNode(rootPage);
			(*ixfileHandle.tree->root)->pageNum = ixfileHandle.root;
			free(rootPage);
			ixfileHandle.tree->nodeMap.insert(make_pair(ixfileHandle.root, ixfileHandle.tree->root));
			if (ixfileHandle.smallestLeaf == ixfileHandle.root)
			{
				ixfileHandle.tree->smallestLeaf = ixfileHandle.tree->root;
			}
			else
			{
				char *smallestLeafPage = (char*)malloc(PAGE_SIZE);
				ixfileHandle.readPage(ixfileHandle.smallestLeaf, smallestLeafPage);
				ixfileHandle.tree->smallestLeaf = ixfileHandle.tree->generateNode(smallestLeafPage);
				(*ixfileHandle.tree->smallestLeaf)->pageNum = ixfileHandle.smallestLeaf;
				free(smallestLeafPage);
				ixfileHandle.tree->nodeMap.insert(make_pair(ixfileHandle.smallestLeaf, ixfileHandle.tree->smallestLeaf));
			}
		}
		ix_ScanIterator.tree = ixfileHandle.tree;
		if (lowKey == NULL)
		{
			ix_ScanIterator.previousIndex = -1;
			ix_ScanIterator.currentNode = (LeafNode**)ixfileHandle.tree->smallestLeaf;
		}
		else
		{
			RID dummyRid;
			LeafEntry leafEntry(attribute.type, lowKey, dummyRid);
			LeafNode** result = NULL;
			if (ixfileHandle.tree->findLeaf(ixfileHandle, leafEntry, result) == -1)
			{
#ifdef DEBUG
				cerr << "Cannot find the leaf node when scanning" << endl;
#endif
				return -1;
			}
			free(leafEntry.key);
			//If the root is NULL
			if (result == NULL)
			{
				ix_ScanIterator.end = true;
				return 0;
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
							if (i - 1 >= 0)
								ix_ScanIterator.previousRID = (*result)->leafEntries[i - 1].rid;
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
							if (i - 1 >= 0)
								ix_ScanIterator.previousRID = (*result)->leafEntries[i - 1].rid;
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
							if (ixfileHandle.tree->loadNode(ixfileHandle, (*result)->rightPointer) == -1)
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
	else
	{
#ifdef DEBUG
		cerr << "The file was not opened when scanning" << endl;
#endif
		return -1;
	}
}

void IndexManager::printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const {
	// if(tree==NULL){
	// 	tree=new BTree();
	// }
	// Node** root_node ;
	// void* data = malloc(PAGE_SIZE);
	// ixfileHandle.readPage(ixfileHandle.root, data);
	// root_node = tree->generateNode((char *)data);

	if (ixfileHandle.tree == NULL)
	{
		ixfileHandle.tree = new BTree();
		ixfileHandle.tree->attrType = attribute.type;

		char *rootPage = (char*)malloc(PAGE_SIZE);
		ixfileHandle.readPage(ixfileHandle.root, rootPage);
		ixfileHandle.tree->root = ixfileHandle.tree->generateNode(rootPage);
		(*ixfileHandle.tree->root)->pageNum = ixfileHandle.root;
		free(rootPage);
		ixfileHandle.tree->nodeMap.insert(make_pair(ixfileHandle.root, ixfileHandle.tree->root));

		if (ixfileHandle.smallestLeaf == ixfileHandle.root)
		{
			ixfileHandle.tree->smallestLeaf = ixfileHandle.tree->root;
		}
		else
		{
			char *smallestLeafPage = (char*)malloc(PAGE_SIZE);
			ixfileHandle.readPage(ixfileHandle.smallestLeaf, smallestLeafPage);
			ixfileHandle.tree->smallestLeaf = ixfileHandle.tree->generateNode(smallestLeafPage);
			(*ixfileHandle.tree->smallestLeaf)->pageNum = ixfileHandle.smallestLeaf;
			free(smallestLeafPage);
			ixfileHandle.tree->nodeMap.insert(make_pair(ixfileHandle.smallestLeaf, ixfileHandle.tree->smallestLeaf));
		}
	}

	Node** root_node;  // Initiat root node
	auto iter = ixfileHandle.tree->nodeMap.find(ixfileHandle.root);
	if (iter != ixfileHandle.tree->nodeMap.end()) {
		root_node = iter->second;
	}
	else {
		void* data = malloc(PAGE_SIZE);
		ixfileHandle.readPage(ixfileHandle.root, data);
		root_node = ixfileHandle.tree->generateNode((char *)data);
		ixfileHandle.tree->root = root_node;
		(*root_node)->pageNum = ixfileHandle.root;
		ixfileHandle.tree->nodeMap.insert(make_pair(ixfileHandle.root, root_node));
		free(data);
	}

	BSF(ixfileHandle, root_node, attribute, 0);
}

void IndexManager::BSF(IXFileHandle &ixfileHandle, Node** cur_node, const Attribute& attribute, int height) const {

	if ((*cur_node)->isLoaded == false) {
		/*void* data = malloc(PAGE_SIZE);
		ixfileHandle.readPage((*cur_node)->pageNum, data);
		cur_node = tree->generateNode((char *)data);*/
		ixfileHandle.tree->loadNode(ixfileHandle, cur_node);
	}
	padding(height);
	cout << "{";
	if ((int)(*cur_node)->pageNum == ixfileHandle.root) {
		//cout<<endl;
		padding(height);
	}
	cout << "\"keys\":";
	cout << "[";
	if ((*cur_node)->nodeType == LeafNodeType) {
		int start = 0;
		LeafEntry first_entry = ((LeafNode*)*cur_node)->leafEntries[0];
		for (size_t i = 0; i <= ((LeafNode*)*cur_node)->leafEntries.size(); i++) {
			if (i == ((LeafNode*)*cur_node)->leafEntries.size()) {
				print_leafkeys(attribute.type, cur_node, start, i - 1);
			}
			else {
				if (compareKey(attribute.type, first_entry.key, ((LeafNode*)*cur_node)->leafEntries[i].key) == 0);
				else {
					print_leafkeys(attribute.type, cur_node, start, i - 1);
					start = i;
					first_entry = ((LeafNode*)*cur_node)->leafEntries[i];
					if (i != ((LeafNode*)*cur_node)->leafEntries.size());
					cout << ",";
				}
			}
			//cout << endl;
		}
		padding(height);
		cout << "]";
	}
	else {
		//generate the cur_node key
		for (size_t i = 0; i < ((InternalNode*)*cur_node)->internalEntries.size(); i++) {
			print_internalkeys(attribute.type, cur_node, i);
			if (i != ((InternalNode*)*cur_node)->internalEntries.size() - 1)
				cout << ",";
			//cout<<endl;
		}
		//padding(height);
		cout << "]";
		//generate the cur_node children
		cout << "," << endl;
		//padding(height);
		cout << "\"children\":[" << endl;

		for (size_t i = 0; i < ((InternalNode*)*cur_node)->internalEntries.size(); i++) {
			BSF(ixfileHandle, ((InternalNode*)*cur_node)->internalEntries[i].leftChild, attribute, height + 1);
			cout << ",";
			if (i == ((InternalNode*)*cur_node)->internalEntries.size() - 1) {
				BSF(ixfileHandle, ((InternalNode*)*cur_node)->internalEntries[i].rightChild, attribute, height + 1);
			}
			//cout<<endl;
		}
		padding(height);
		cout << "]";
	}
	if ((int)(*cur_node)->pageNum == ixfileHandle.root) {
		cout << endl;
		//padding(height);
	}
	cout << "}";
	cout << endl;

}

void IndexManager::padding(int height) const {
	for (int i = 0; i < height; i++) {
		cout << "  ";
	}
}
void IndexManager::print_leafkeys(AttrType attrType, Node** cur_node, int start, int end) const {
	cout << '"';
	if (attrType == TypeInt) {
		int key1;
		memcpy(&key1, (char *)(((LeafNode*)*cur_node)->leafEntries[start].key), 4);
		cout << key1 << ":[";
	}
	else if (attrType == TypeVarChar) {
		int length;
		memcpy(&length, ((LeafNode*)*cur_node)->leafEntries[start].key, sizeof(int));
		string str((char *)((LeafNode*)*cur_node)->leafEntries[start].key + sizeof(int), length);
		cout << str << ":[";
	}
	else {
		float key1;
		memcpy(&key1, (char *)(((LeafNode*)*cur_node)->leafEntries[start].key), 4);
		cout << key1 << ":[";
	}
	for (int i = start; i <= end; i++) {
		if (i == end) {
			cout << "(" << ((LeafNode*)*cur_node)->leafEntries[i].rid.pageNum << "," << ((LeafNode*)*cur_node)->leafEntries[i].rid.slotNum << ")";
		}
		else {
			cout << "(" << ((LeafNode*)*cur_node)->leafEntries[i].rid.pageNum << "," << ((LeafNode*)*cur_node)->leafEntries[i].rid.slotNum << "),";
		}
	}
	cout << "]";
	cout << '"';
}
void IndexManager::print_internalkeys(AttrType attrType, Node** cur_node, int index) const {
	cout << '"';
	if (attrType == TypeInt) {
		int key1;
		memcpy(&key1, ((InternalNode*)*cur_node)->internalEntries[index].key, sizeof(int));
		cout << key1 << '"';
	}
	else if (attrType == TypeVarChar) {
		int length;
		memcpy(&length, ((InternalNode*)*cur_node)->internalEntries[index].key, sizeof(int));
		string str((char *)((InternalNode*)*cur_node)->internalEntries[index].key + sizeof(int), length);
		cout << str << '"';
	}
	else {
		float key1;
		memcpy(&key1, ((InternalNode*)*cur_node)->internalEntries[index].key, sizeof(float));
		cout << key1 << '"';
	}

}

int IndexManager::compareEntry(AttrType attrType, const LeafEntry &pair1, const LeafEntry &pair2) const {
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
		return floor(*(float*)v1 - *(float*)v2);
	}
	else if (this->attrType == AttrType::TypeVarChar)
	{
		int strLength1 = *(int*)v1;
		int strLength2 = *(int*)v2;
		string s1((char*)v1 + sizeof(int), strLength1);
		string s2((char*)v2 + sizeof(int), strLength2);
		return s1.compare(s2);
	}
	return 0;
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
		//Fetch the entry to the result
		rid = (*this->currentNode)->leafEntries[currentIndex].rid;
		memcpy(key, (*this->currentNode)->leafEntries[currentIndex].key, (*this->currentNode)->leafEntries[currentIndex].size);
		//If the current key is the last key in leaf node
		if (currentIndex == (int)(*this->currentNode)->leafEntries.size() - 1)
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
	tree = NULL;
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

LeafEntry::LeafEntry()
{
	this->key = NULL;
	rid.pageNum = -1;
	rid.slotNum = -1;
	this->size = 0;
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

LeafEntry::~LeafEntry()
{
}

InternalEntry::InternalEntry()
{
	this->key = NULL;
	this->leftChild = NULL;
	this->rightChild = NULL;
}

InternalEntry::InternalEntry(const AttrType &attribute, const void* key, Node** leftChild, Node** rightChild)
{
	if (attribute == AttrType::TypeInt)
	{
		this->key = malloc(sizeof(int));
		this->size = sizeof(int);
		memcpy(this->key, key, sizeof(int));
	}
	else if (attribute == AttrType::TypeReal)
	{
		this->key = malloc(sizeof(float));
		this->size = sizeof(float);
		memcpy(this->key, key, sizeof(float));
	}
	else if (attribute == AttrType::TypeVarChar)
	{
		int strLength = *(int*)key;
		this->key = malloc(sizeof(int) + strLength);
		this->size = sizeof(int) + strLength;
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

InternalEntry::~InternalEntry()
{
}

Node::Node()
{
	this->isDirty = false;
	this->isLoaded = false;
	this->nodeSize = 0;
	this->nodeType = UnknownNodeType;
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
	for (auto &i : this->internalEntries)
	{
		if (i.leftChild && *i.leftChild && (*i.leftChild)->nodeType == UnknownNodeType)
		{
			delete *i.leftChild;
			delete i.leftChild;
		}
		if (i.rightChild && *i.rightChild && (*i.rightChild)->nodeType == UnknownNodeType)
		{
			delete *i.rightChild;
			delete i.rightChild;
		}
		free(i.key);
	}
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
	if (this->rightPointer && *this->rightPointer && (*this->rightPointer)->nodeType == UnknownNodeType)
	{
		delete *this->rightPointer;
		delete this->rightPointer;
	}
	if (this->overflowPointer && *this->overflowPointer && (*this->overflowPointer)->nodeType == UnknownNodeType)
	{
		delete *this->overflowPointer;
		delete this->overflowPointer;
	}
	for (auto &i : this->leafEntries)
	{
		free(i.key);
	}
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
		if (item.second && *item.second)
		{
			delete *item.second;
			*item.second = NULL;
		}
	}
	for (auto &item : this->nodeMap)
	{
		if (item.second)
		{
			delete item.second;
		}
	}
}

RC BTree::insertEntry(IXFileHandle &ixfileHandle, const LeafEntry &pair) {
	int entry_length;

	if (this->attrType == TypeVarChar) {

		memcpy(&entry_length, (char *)pair.key, 4);
		entry_length += 12;
	}
	else {
		entry_length = 12;

	}

	if (root == NULL) {  //insert into the first node in the new tree
						 //set node fields
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
		free(new_page);
		nodeMap.insert(make_pair(ixfileHandle.ixAppendPageCounter - 1, new_node));
		((LeafNode*)*new_node)->pageNum = ixfileHandle.ixAppendPageCounter - 1;
		//set meta fields
		ixfileHandle.root = ixfileHandle.ixAppendPageCounter - 1;
		ixfileHandle.smallestLeaf = ixfileHandle.ixAppendPageCounter - 1;
		//ixfileHandle.writeMetaPage();
	}
	else {
		//Node** root_node;  // Initiat root node
		//unordered_map<PageNum, Node**>::iterator iter; //judge the curnode exist in the hashmap
		//iter = nodeMap.find(ixfileHandle.root);
		//if (iter != nodeMap.end()) {
		//	root_node = iter->second;
		//}
		//else {
		//	void* data = malloc(PAGE_SIZE);
		//	ixfileHandle.readPage(ixfileHandle.root, data);
		//	root_node = generateNode((char *)data);
		//	nodeMap.insert(make_pair(ixfileHandle.root, root_node));
		//	free(data);
		//}
		Node** tar_leafNode = findLeafnode(ixfileHandle, pair, this->root);     //find the inserted leaf node
		if (((LeafNode*)*tar_leafNode)->isLoaded == false) {
			/*void* data = malloc(PAGE_SIZE);
			PageNum cur_pageNum = ((LeafNode*)*tar_leafNode)->pageNum;
			ixfileHandle.readPage(cur_pageNum, data);
			tar_leafNode = generateNode((char *)data);
			nodeMap.insert(make_pair(cur_pageNum, tar_leafNode));
			free(data);*/
			loadNode(ixfileHandle, tar_leafNode);
		}
		//first insert the entry to the origin leafnode
		auto iter_vector = ((LeafNode*)*tar_leafNode)->leafEntries.begin();   //find the right position in leafnode
		int isadded = 0;
		for (size_t i = 0; i < ((LeafNode*)*tar_leafNode)->leafEntries.size(); i++) {
			if (compareEntry(pair, ((LeafNode*)*tar_leafNode)->leafEntries[i]) < 0) {
				((LeafNode*)*tar_leafNode)->leafEntries.insert(iter_vector + i, pair);
				isadded = 1;
				break;
			}
		}
		if (isadded == 0) {
			((LeafNode*)*tar_leafNode)->leafEntries.push_back(pair);
		}
		((LeafNode*)*tar_leafNode)->nodeSize += entry_length + sizeof(OffsetType);
		if (((LeafNode*)*tar_leafNode)->nodeSize <= PAGE_SIZE) {   //insert into node into space-enough node
																   //write the updated origin leaf node into file
			void* new_page = generatePage(tar_leafNode);
			ixfileHandle.writePage(((LeafNode*)*tar_leafNode)->pageNum, new_page);

			free(new_page);
			return 0;
		}
		else {                                                        //split node;
			Node** new_node = new Node*;
			*new_node = new LeafNode();
			((LeafNode*)*new_node)->isLoaded = true;

			if (((LeafNode*)*tar_leafNode)->leafEntries.size() <= 1)
				return -1;

			//judge  the same key;
			int mid_index = ((LeafNode*)*tar_leafNode)->leafEntries.size() / 2;
			int back = 0;
			for (size_t i = mid_index; i <= ((LeafNode*)*tar_leafNode)->leafEntries.size(); i++) {
				if (i == (((LeafNode*)*tar_leafNode)->leafEntries.size())) {
					back = 1;
					break;
				}
				if (compareKey(((LeafNode*)*tar_leafNode)->leafEntries[i].key, ((LeafNode*)*tar_leafNode)->leafEntries[i - 1].key) == 0);
				else {
					mid_index = i;
					break;
				}
			}
			if (back == 1) {
				for (int i = mid_index; i >= 0; i--) {
					if (i == 0) {
						return -1;
					}
					if (compareKey(((LeafNode*)*tar_leafNode)->leafEntries[i].key, ((LeafNode*)*tar_leafNode)->leafEntries[i - 1].key) == 0);
					else {
						mid_index = i;
						break;
					}
				}
			}

			//move the origin leafnode the second half records to the new leafnode, and update the nodesize
			for (size_t i = mid_index; i < ((LeafNode*)*tar_leafNode)->leafEntries.size(); i++) {
				LeafEntry leafEntry(this->attrType, ((LeafNode*)*tar_leafNode)->leafEntries[i].key, ((LeafNode*)*tar_leafNode)->leafEntries[i].rid);
				((LeafNode*)*new_node)->leafEntries.push_back(leafEntry);
				int var_length;
				if (attrType == TypeVarChar) {
					memcpy(&var_length, (char *)((LeafNode*)*tar_leafNode)->leafEntries[i].key, 4);
					var_length += 12;

				}
				else {
					var_length = 12;
				}
				((LeafNode*)*tar_leafNode)->nodeSize -= var_length + sizeof(OffsetType);
				((LeafNode*)*new_node)->nodeSize += var_length + sizeof(OffsetType);
			}
			//the mid_node key's length

			int var_length;
			//void* key_mid;
			if (attrType == TypeVarChar) {
				memcpy(&var_length, (char *)((LeafNode*)*tar_leafNode)->leafEntries[mid_index].key, 4);
				var_length += 4;
				// key_mid = malloc(sizeof(int) + var_length);
				// memcpy((char *)key_mid, (char *)((LeafNode*)*tar_leafNode)->leafEntries[mid_index].key, sizeof(int) + var_length);
			}
			else {
				var_length = 4;
				// key_mid = malloc(sizeof(int));
				// memcpy((char *)key_mid, (char *)((LeafNode*)*tar_leafNode)->leafEntries[mid_index].key, sizeof(int));
			}
			//free(key_mid);
			//delete the second half records from the origin leafnode.
			//for (int i = mid_index; i < ((LeafNode*)*tar_leafNode)->leafEntries.size(); i++) {

			for (auto i = ((LeafNode*)*tar_leafNode)->leafEntries.begin() + mid_index; i != ((LeafNode*)*tar_leafNode)->leafEntries.end(); i++)
				free(i->key);
			((LeafNode*)*tar_leafNode)->leafEntries.erase(((LeafNode*)*tar_leafNode)->leafEntries.begin() + mid_index, ((LeafNode*)*tar_leafNode)->leafEntries.end());

			//}
			//update the origin and new node's rightpointer
			// ((LeafNode*)*new_node)->rightPointer = ((LeafNode*)*tar_leafNode)->rightPointer;
			// ((LeafNode*)*tar_leafNode)->rightPointer = new_node;
			// ((LeafNode*)*new_node)->nodeType = LeafNodeType;
			//if it dont have the parentNode,create a parentNode
			if ((((LeafNode*)*tar_leafNode)->parentPointer) == NULL) {
				((LeafNode*)*new_node)->pageNum = ixfileHandle.ixAppendPageCounter + 1;  //assume the new leaf node will append in ixappendpageCounter th
				InternalEntry internal_pair(attrType, ((LeafNode*)*new_node)->leafEntries[0].key, tar_leafNode, new_node);
				Node** new_parentNode = new Node*;
				*new_parentNode = new InternalNode();
				((InternalNode*)*new_parentNode)->isLoaded = true;
				((InternalNode*)*new_parentNode)->internalEntries.push_back(internal_pair);
				((InternalNode*)*new_parentNode)->nodeType = InternalNodeType;
				((InternalNode*)*new_parentNode)->nodeSize += var_length + 2 * sizeof(int) + sizeof(OffsetType);
				//write the father node to the file
				void* new_page = generatePage(new_parentNode);
				ixfileHandle.appendPage(new_page);
				free(new_page);
				nodeMap.insert(make_pair(ixfileHandle.ixAppendPageCounter - 1, new_parentNode));
				((InternalNode*)*new_parentNode)->pageNum = ixfileHandle.ixAppendPageCounter - 1;


				//update the origin and new node's rightpointer
				((LeafNode*)*new_node)->rightPointer = ((LeafNode*)*tar_leafNode)->rightPointer;
				((LeafNode*)*tar_leafNode)->rightPointer = new_node;
				((LeafNode*)*new_node)->nodeType = LeafNodeType;
				//set father-son relatoinship.
				((LeafNode*)*tar_leafNode)->parentPointer = new_parentNode;
				((LeafNode*)*new_node)->parentPointer = new_parentNode;
				//set meta fields
				ixfileHandle.root = ixfileHandle.ixAppendPageCounter - 1;
				root = new_parentNode;
				//ixfileHandle.writeMetaPage();
				//update the origin leafnode to the file
				void* new_originLeafpage = generatePage(tar_leafNode);
				int originLeafpage = ((LeafNode*)*tar_leafNode)->pageNum;
				ixfileHandle.writePage(originLeafpage, new_originLeafpage);
				free(new_originLeafpage);
				//add the updated leafnode to the file

				void* new_newLeafpage = generatePage(new_node);
				ixfileHandle.appendPage(new_newLeafpage);
				nodeMap.insert(make_pair(ixfileHandle.ixAppendPageCounter - 1, new_node));
				((LeafNode*)*new_node)->pageNum = ixfileHandle.ixAppendPageCounter - 1;
				free(new_newLeafpage);
				return 0;
			}
			else {  // it have the parentNode
				Node** origin_parentNode = NULL;
				PageNum origin_parentNode_pagenum = ((InternalNode*)*(((LeafNode*)*tar_leafNode)->parentPointer))->pageNum;
				auto iter = nodeMap.find(origin_parentNode_pagenum);
				if (iter != nodeMap.end()) {
					origin_parentNode = iter->second;
				}
				else {
					void* data3 = malloc(PAGE_SIZE);
					ixfileHandle.readPage(origin_parentNode_pagenum, data3);
					origin_parentNode = generateNode((char *)data3);
					nodeMap.insert(make_pair(origin_parentNode_pagenum, origin_parentNode));
					free(data3);
				}

				//update the origin and new node's rightpointer
				((LeafNode*)*new_node)->rightPointer = ((LeafNode*)*tar_leafNode)->rightPointer;
				((LeafNode*)*new_node)->pageNum = ixfileHandle.ixAppendPageCounter;
				((LeafNode*)*tar_leafNode)->rightPointer = new_node;
				((LeafNode*)*new_node)->nodeType = LeafNodeType;

				//update the origin leafnode to the file
				void* new_originLeafpage = generatePage(tar_leafNode);
				int originLeafpage = ((LeafNode*)*tar_leafNode)->pageNum;
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
				auto internal_iter_vector = ((InternalNode*)*origin_parentNode)->internalEntries.begin();   //find the right position in leafnode
				int isadded1 = 0;
				int added_index = 0;
				for (size_t i = 0; i < ((InternalNode*)*origin_parentNode)->internalEntries.size(); i++) {
					if (compareKey(internal_pair.key, ((InternalNode*)*origin_parentNode)->internalEntries[i].key) < 0) {
						((InternalNode*)*origin_parentNode)->internalEntries.insert(internal_iter_vector + i, internal_pair);
						isadded1 = 1;
						added_index = i;
						break;
					}
				}
				if (isadded1 == 0) {
					((InternalNode*)*origin_parentNode)->internalEntries.push_back(internal_pair);
					((InternalNode*)*origin_parentNode)->internalEntries[((InternalNode*)*origin_parentNode)->internalEntries.size() - 2].rightChild = internal_pair.leftChild;
					((InternalNode*)*origin_parentNode)->nodeSize += var_length + sizeof(int) + sizeof(OffsetType);
				}
				else if (added_index == 0) {
					((InternalNode*)*origin_parentNode)->internalEntries[1].leftChild = internal_pair.rightChild;
					((InternalNode*)*origin_parentNode)->nodeSize += var_length + sizeof(int) + sizeof(OffsetType);
				}
				else {
					((InternalNode*)*origin_parentNode)->internalEntries[added_index - 1].rightChild = internal_pair.leftChild;
					((InternalNode*)*origin_parentNode)->internalEntries[added_index + 1].leftChild = internal_pair.rightChild;
					((InternalNode*)*origin_parentNode)->nodeSize += var_length + sizeof(OffsetType);
				}
				//((InternalNode*)*origin_parentNode)->nodeSize += var_length + 2 * sizeof(int) + sizeof(OffsetType);
				// if(((LeafNode*)*origin_parentNode)->nodeSize + var_length +2*sizeof(int)+sizeof(OffsetType)<=PAGE_SIZE){
				// }else{ //split the parentNode

				// }
				//
				if (((InternalNode*)*origin_parentNode)->nodeSize <= PAGE_SIZE) {
					void* new_page = generatePage(origin_parentNode);
					ixfileHandle.writePage(((InternalNode*)*origin_parentNode)->pageNum, new_page);
					free(new_page);
					return 0;
				}
				while (((InternalNode*)*origin_parentNode)->nodeSize > PAGE_SIZE) {   //split the parent node
					Node** new_parentNode = new Node*;
					*new_parentNode = new InternalNode();
					((InternalNode*)*new_parentNode)->isLoaded = true;
					((InternalNode*)*new_parentNode)->nodeSize += sizeof(int);
					//move the origin leafnode the second half records to the new leafnode, and update the nodesize
					for (size_t i = ((InternalNode*)*origin_parentNode)->internalEntries.size() / 2 + 1; i < ((InternalNode*)*origin_parentNode)->internalEntries.size(); i++) {
						InternalEntry internalEntry(this->attrType, ((InternalNode*)*origin_parentNode)->internalEntries[i].key, ((InternalNode*)*origin_parentNode)->internalEntries[i].leftChild, ((InternalNode*)*origin_parentNode)->internalEntries[i].rightChild);
						((InternalNode*)*new_parentNode)->internalEntries.push_back(internalEntry);
						if (attrType == TypeVarChar) {
							memcpy(&var_length, (char *)((InternalNode*)*origin_parentNode)->internalEntries[i].key, 4);
							var_length += 4;
						}
						else {
							var_length = 4;
						}
						((InternalNode*)*origin_parentNode)->nodeSize -= var_length + sizeof(OffsetType) + sizeof(int);
						((InternalNode*)*new_parentNode)->nodeSize += var_length + sizeof(OffsetType) + sizeof(int);
					}
					//the mid_node key's length
					int mid_index = ((InternalNode*)*origin_parentNode)->internalEntries.size() / 2;
					void* key_mid1;
					if (attrType == TypeVarChar) {
						memcpy(&var_length, (char *)((InternalNode*)*origin_parentNode)->internalEntries[mid_index].key, 4);
						key_mid1 = malloc(sizeof(int) + var_length);
						var_length += 4;
						memcpy((char *)key_mid1, (char *)((InternalNode*)*origin_parentNode)->internalEntries[mid_index].key, var_length);
					}
					else {
						var_length = 4;
						key_mid1 = malloc(sizeof(int));
						memcpy((char *)key_mid1, (char *)((InternalNode*)*origin_parentNode)->internalEntries[mid_index].key, sizeof(int));
					}
					//delete the second half records from the origin leafnode.
					//for (int i = mid_index; i < ((InternalNode*)*origin_parentNode)->internalEntries.size(); i++) {
					for (auto i = ((InternalNode*)*origin_parentNode)->internalEntries.begin() + mid_index; i != ((InternalNode*)*origin_parentNode)->internalEntries.end(); i++)
						free(i->key);
					((InternalNode*)*origin_parentNode)->internalEntries.erase(((InternalNode*)*origin_parentNode)->internalEntries.begin() + mid_index, ((InternalNode*)*origin_parentNode)->internalEntries.end());
					//}
					//update the origin and new node's rightpointer
					((InternalNode*)*new_parentNode)->nodeType = InternalNodeType;
					if ((((InternalNode*)*origin_parentNode)->parentPointer) == NULL) {
						((InternalNode*)*new_parentNode)->pageNum = ixfileHandle.ixAppendPageCounter + 1;  //assume the new leaf node will append in ixappendpageCounter th
						InternalEntry internal_pair1(attrType, key_mid1, origin_parentNode, new_parentNode);
						Node** new_grandparentNode = new Node*;
						*new_grandparentNode = new InternalNode();
						((InternalNode*)*new_grandparentNode)->isLoaded = true;
						((InternalNode*)*new_grandparentNode)->internalEntries.push_back(internal_pair1);
						((InternalNode*)*new_grandparentNode)->nodeType = InternalNodeType;
						((InternalNode*)*new_grandparentNode)->nodeSize += var_length + 2 * sizeof(int) + sizeof(OffsetType);
						//write the father node to the file
						void* new_page = generatePage(new_grandparentNode);
						ixfileHandle.appendPage(new_page);
						nodeMap.insert(make_pair(ixfileHandle.ixAppendPageCounter - 1, new_grandparentNode));
						((InternalNode*)*new_grandparentNode)->pageNum = ixfileHandle.ixAppendPageCounter - 1;
						free(new_page);
						//set father-son relatoinship.
						((InternalNode*)*origin_parentNode)->parentPointer = new_grandparentNode;
						((InternalNode*)*new_parentNode)->parentPointer = new_grandparentNode;
						//set meta fields
						ixfileHandle.root = ixfileHandle.ixAppendPageCounter - 1;
						root = new_grandparentNode;
						//ixfileHandle.writeMetaPage();
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
						//update the news;
						for (size_t i = 0; i < ((InternalNode*)*new_parentNode)->internalEntries.size(); i++) {
							(*(((InternalNode*)*new_parentNode)->internalEntries[i].leftChild))->parentPointer = new_parentNode;
							void* datal = generatePage(((InternalNode*)*new_parentNode)->internalEntries[i].leftChild);
							int leftpage = (*(((InternalNode*)*new_parentNode)->internalEntries[i].leftChild))->pageNum;
							ixfileHandle.writePage(leftpage, datal);
							free(datal);
							(*(((InternalNode*)*new_parentNode)->internalEntries[i].rightChild))->parentPointer = new_parentNode;
							void* datar = generatePage(((InternalNode*)*new_parentNode)->internalEntries[i].rightChild);
							int rightpage = (*(((InternalNode*)*new_parentNode)->internalEntries[i].rightChild))->pageNum;
							ixfileHandle.writePage(rightpage, datar);
							free(datar);
						}

						free(new_newLeafpage);
						free(key_mid1);
						return 0;
					}
					else {  // it have the parentNode
						Node** origin_grandparentNode = NULL;
						PageNum origin_grandparentNode_pagenum = ((InternalNode*)*(((InternalNode*)*origin_parentNode)->parentPointer))->pageNum;
						iter = nodeMap.find(origin_grandparentNode_pagenum);
						if (iter != nodeMap.end()) {
							origin_grandparentNode = iter->second;
						}
						else {
							void* data2 = malloc(PAGE_SIZE);
							ixfileHandle.readPage(origin_grandparentNode_pagenum, data2);
							origin_grandparentNode = generateNode((char *)data2);
							free(data2);
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
						//update the new parent entry's parentpointer
						for (size_t i = 0; i < ((InternalNode*)*new_parentNode)->internalEntries.size(); i++) {
							(*(((InternalNode*)*new_parentNode)->internalEntries[i].leftChild))->parentPointer = new_parentNode;
							void* datal = generatePage(((InternalNode*)*new_parentNode)->internalEntries[i].leftChild);
							int leftpage = (*(((InternalNode*)*new_parentNode)->internalEntries[i].leftChild))->pageNum;
							ixfileHandle.writePage(leftpage, datal);
							free(datal);
							(*(((InternalNode*)*new_parentNode)->internalEntries[i].rightChild))->parentPointer = new_parentNode;
							void* datar = generatePage(((InternalNode*)*new_parentNode)->internalEntries[i].rightChild);
							int rightpage = (*(((InternalNode*)*new_parentNode)->internalEntries[i].rightChild))->pageNum;
							ixfileHandle.writePage(rightpage, datar);
							free(datar);
						}


						InternalEntry internal_pair1(attrType, key_mid1, origin_parentNode, new_parentNode);
						free(key_mid1);
						//add the entry into to internalnode
						auto internal_iter_vector = ((InternalNode*)*origin_grandparentNode)->internalEntries.begin();   //find the right position in leafnode
						int isadded1 = 0;
						int added_index = 0;
						for (size_t i = 0; i < ((InternalNode*)*origin_grandparentNode)->internalEntries.size(); i++) {

							if (compareKey(internal_pair1.key, ((InternalNode*)*origin_grandparentNode)->internalEntries[i].key) < 0) {
								((InternalNode*)*origin_grandparentNode)->internalEntries.insert(internal_iter_vector + i, internal_pair1);
								isadded1 = 1;
								added_index = i;
								break;
							}
						}
						if (isadded1 == 0) {
							((InternalNode*)*origin_grandparentNode)->internalEntries.push_back(internal_pair1);
							((InternalNode*)*origin_grandparentNode)->internalEntries[((InternalNode*)*origin_grandparentNode)->internalEntries.size() - 2].rightChild = internal_pair1.leftChild;
							((InternalNode*)*origin_grandparentNode)->nodeSize += var_length + sizeof(int) + sizeof(OffsetType);
						}
						else if (added_index == 0) {
							((InternalNode*)*origin_grandparentNode)->internalEntries[1].leftChild = internal_pair1.rightChild;
							((InternalNode*)*origin_grandparentNode)->nodeSize += var_length + sizeof(int) + sizeof(OffsetType);
						}
						else {
							((InternalNode*)*origin_grandparentNode)->internalEntries[added_index - 1].rightChild = internal_pair1.leftChild;
							((InternalNode*)*origin_grandparentNode)->internalEntries[added_index + 1].leftChild = internal_pair1.rightChild;
							((InternalNode*)*origin_grandparentNode)->nodeSize += var_length + sizeof(OffsetType);
						}
						//((InternalNode*)*origin_grandparentNode)->nodeSize += var_length + 2 * sizeof(int) + sizeof(OffsetType);
						if (((InternalNode*)*origin_grandparentNode)->nodeSize <= PAGE_SIZE) {
							void* new_page = generatePage(origin_grandparentNode);
							ixfileHandle.writePage(((InternalNode*)*origin_grandparentNode)->pageNum, new_page);
							free(new_page);
							return 0;
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
		int size = ((InternalNode*)*cur_node)->internalEntries.size();
		if (attrType == TypeInt) {                    // key is int
			int key;
			memcpy(&key, (char *)pair.key, sizeof(int));
			for (int i = 0; i <= size; i++) {
				if (i == size) {
					ixfileHandle.ixReadPageCounter++;
					cur_node = ((InternalNode*)*cur_node)->internalEntries[size - 1].rightChild;
					if (((InternalNode*)*cur_node)->isLoaded == false) {
						loadNode(ixfileHandle, cur_node);
					}
					break;
				}
				else {
					int sou_key;
					memcpy(&sou_key, ((InternalNode*)*cur_node)->internalEntries[i].key, sizeof(int));

					if (key < sou_key) {
						ixfileHandle.ixReadPageCounter++;
						cur_node = ((InternalNode*)*cur_node)->internalEntries[i].leftChild;
						if (((InternalNode*)*cur_node)->isLoaded == false) {
							loadNode(ixfileHandle, cur_node);
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
					ixfileHandle.ixReadPageCounter++;
					cur_node = ((InternalNode*)*cur_node)->internalEntries[size - 1].rightChild;
					if (((InternalNode*)*cur_node)->isLoaded == false) {
						loadNode(ixfileHandle, cur_node);
					}
					break;
				}
				else {
					float sou_key;
					memcpy(&sou_key, ((InternalNode*)*cur_node)->internalEntries[i].key, sizeof(float));
					if (key < sou_key) {
						ixfileHandle.ixReadPageCounter++;
						cur_node = ((InternalNode*)*cur_node)->internalEntries[i].leftChild;
						if (((InternalNode*)*cur_node)->isLoaded == false) {
							loadNode(ixfileHandle, cur_node);
						}
						break;
					}
				}
			}
		}
		else if (attrType == TypeVarChar) {
			for (int i = 0; i <= size; i++) {
				if (i == size) {
					ixfileHandle.ixReadPageCounter++;
					cur_node = ((InternalNode*)*cur_node)->internalEntries[size - 1].rightChild;
					if (((InternalNode*)*cur_node)->isLoaded == false) {
						loadNode(ixfileHandle, cur_node);
					}
					break;
				}
				else {
					if (compareKey(pair.key, ((InternalNode*)*cur_node)->internalEntries[i].key) < 0) {
						ixfileHandle.ixReadPageCounter++;
						cur_node = ((InternalNode*)*cur_node)->internalEntries[i].leftChild;
						if (((InternalNode*)*cur_node)->isLoaded == false) {
							loadNode(ixfileHandle, cur_node);
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
		return floor(*(float*)v1 - *(float*)v2);
	}
	else if (this->attrType == AttrType::TypeVarChar)
	{
		int strLength1 = *(int*)v1;
		int strLength2 = *(int*)v2;
		string s1((char*)v1 + sizeof(int), strLength1);
		string s2((char*)v2 + sizeof(int), strLength2);
		return s1.compare(s2);
	}
	return 0;
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
				{
					free(i->key);
					((InternalNode*)*node)->internalEntries.erase(i);
				}
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
				free(i->key);
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
#ifdef DEBUG
	cerr << "The node is UNKNOWN when removing entry from the node" << endl;
#endif
	return -1;
}

OffsetType BTree::getEntrySize(int nodeType, const LeafEntry &pair, bool isLastEntry)
{
	if (nodeType == InternalNodeType)
	{
		OffsetType result = sizeof(PageNum); //size of left child page num
		if (isLastEntry)
			result += sizeof(PageNum); //size of right child page num
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
	else
	{
#ifdef DEBUG
		cerr << "Unknown type of entry " << pair.rid.pageNum << ", " << pair.rid.slotNum << " when getting entry size" << endl;
#endif
		return 0;
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
		result = NULL;
		return 0;
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
		while (i < (int)((InternalNode*)*currentNode)->internalEntries.size())
		{
			if (compareKey(((InternalNode*)*currentNode)->internalEntries[i].key, pair.key) <= 0)
				++i;
			else
				break;
		}
		Node** child = NULL;
		i != (int)((InternalNode*)*currentNode)->internalEntries.size() ? child = ((InternalNode*)*currentNode)->internalEntries[i].leftChild : child = ((InternalNode*)*currentNode)->internalEntries.back().rightChild;
		if (child)
		{
			if (*child)
			{
				if (!(*child)->isLoaded)
				{
					//If the node was not loaded before, we need to load it from page file
					if (loadNode(ixfileHandle, child) == -1)
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
				currentNode = child;
			}
			else
			{
#ifdef DEBUG
				cerr << "The child points to a released memory when finding the leaf" << endl;
#endif
				return -1;
			}
		}
		else
		{
#ifdef DEBUG
			cerr << "The child is null when finding the leaf" << endl;
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
		//Node** temp = this->root;
		//this->nodeMap.erase((*this->root)->pageNum);
		this->root = ((InternalNode*)*this->root)->internalEntries[0].rightChild;
		(*this->root)->parentPointer = NULL;
		ixfileHandle.root = (*this->root)->pageNum;

		/*this->nodeMap.erase((*temp)->pageNum);
		delete *temp;
		*temp = NULL;*/
	}
	else
	{
		// If it is a leaf (has no children), then the whole tree is empty.
		/*this->nodeMap.erase((*this->root)->pageNum);
		delete *this->root;*/
		this->root = NULL;
		ixfileHandle.root = NULLNODE;
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

	size_t internalEntriesSize = ((InternalNode*)*((*node)->parentPointer))->internalEntries.size();
	for (size_t i = 0; i < internalEntriesSize; i++)
	{
		if (*((InternalNode*)*((*node)->parentPointer))->internalEntries[i].leftChild == *node)
		{
			result = i - 1;
			return 0;
		}
	}
	if (*((InternalNode*)*((*node)->parentPointer))->internalEntries[internalEntriesSize - 1].rightChild == *node)
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
	return 0;
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
		size_t node2EntriesSize = (*node2)->nodeSize;
		node2EntriesSize -= sizeof(MarkType) - sizeof(OffsetType) - sizeof(int); //Decrease size of nodeType, usedSpace, and parent pageNum
		node2EntriesSize -= sizeof(int); //Derease size of a pointer in node2
		node2EntriesSize -= sizeof(OffsetType); //Decrease size of slotCount in slot table
		result = (*node1)->nodeSize + node2EntriesSize;
		result += sizeof(int) + sizeOfParentKey + sizeof(int); //We need to add the key between nodes' pointers from their parent node
		return 0;
	}
	else if ((*node1)->nodeType == LeafNodeType && (*node2)->nodeType == LeafNodeType)
	{
		size_t node2EntriesSize = (*node2)->nodeSize;
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

	//Update the leftchild pointer of the entry which after the middle entry
	if (keyIndex < (int)((InternalNode*)*(*node)->parentPointer)->internalEntries.size() - 1)
	{
		((InternalNode*)*(*node)->parentPointer)->internalEntries[keyIndex + 1].leftChild = neighbor;
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
	/*this->nodeMap.erase((*node)->pageNum);
	delete *node;
	*node = NULL;*/
	(*node)->isLoaded = false;
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
			{
#ifdef DEBUG
				cout << "The finalNodeSize = " << finalNodeSize << " is too large when redistributing InternalNode neighbor and node" << endl;
#endif
				return 0;
			}

			//Copy the rightmost key in the neighbor node and insert it in the front of node
			//The value of the borrowed key is the middle key value
			InternalEntry borrowedEntry(this->attrType, ((InternalNode*)*(*node)->parentPointer)->internalEntries[keyIndex].key);
			borrowedEntry.leftChild = ((InternalNode*)*neighbor)->internalEntries.back().rightChild;
			borrowedEntry.rightChild = ((InternalNode*)*node)->internalEntries[0].leftChild;
			(*borrowedEntry.leftChild)->parentPointer = node;

			//If the entry that we decide to update in parent node is too big, we cannot and do not need to redistribute either
			OffsetType sizeOfKeyInParent = getKeySize(((InternalNode*)*neighbor)->internalEntries.back().key);
			OffsetType finalParentSize = ((InternalNode*)*(*node)->parentPointer)->nodeSize + sizeOfKeyInParent - getKeySize(((InternalNode*)*(*node)->parentPointer)->internalEntries[keyIndex].key);
			if (finalParentSize > PAGE_SIZE)
			{
#ifdef DEBUG
				cout << "The finalParentSize = " << finalParentSize << " is too large when redistributing InternalNode neighbor and node" << endl;
#endif
				return 0;
			}

			//Update the middle key value in the parent node into the value of rightmost key in the neighbor node
			InternalEntry parentEntry(this->attrType, ((InternalNode*)*neighbor)->internalEntries.back().key);
			parentEntry.leftChild = ((InternalNode*)*(*node)->parentPointer)->internalEntries[keyIndex].leftChild;
			parentEntry.rightChild = ((InternalNode*)*(*node)->parentPointer)->internalEntries[keyIndex].rightChild;

			//Insert the borrowed entry, update parent node, and delete the entry in the neighbor node
			((InternalNode*)*node)->internalEntries.insert(((InternalNode*)*node)->internalEntries.begin(), borrowedEntry);
			((InternalNode*)*(*node)->parentPointer)->internalEntries[keyIndex] = parentEntry;
			free(((InternalNode*)*neighbor)->internalEntries.back().key);
			((InternalNode*)*neighbor)->internalEntries.erase(((InternalNode*)*neighbor)->internalEntries.end() - 1);
		}
		else if ((*node)->nodeType == LeafNodeType && (*neighbor)->nodeType == LeafNodeType)
		{
			//Count the leftmost same keys in the neighbor
			int sameKeyCount = 0;
			void * startKey = ((LeafNode*)*neighbor)->leafEntries.back().key;
			for (auto i = ((LeafNode*)*neighbor)->leafEntries.rbegin(); i != ((LeafNode*)*neighbor)->leafEntries.rend(); i++)
			{
				if (compareKey(startKey, i->key) == 0)
					sameKeyCount++;
				else
					break;
			}
			//If neighbor only has one kind of keys, we cannot and do not need to redistribute either
			if (sameKeyCount == (int)((LeafNode*)*neighbor)->leafEntries.size())
			{
#ifdef DEBUG
				cout << "The sameKeyCount = " << sameKeyCount << " when redistributing LeafNode node and neighbor" << endl;
#endif
				return 0;
			}

			//Copy the rightmost key in the neighbor node and insert it in the front of node
			LeafEntry firstBorrowedEntry(this->attrType, ((LeafNode*)*neighbor)->leafEntries.back().key, ((LeafNode*)*neighbor)->leafEntries.back().rid);

			//If the entry that we decide to borrow from neighbor is too big, we cannot and do not need to redistribute either
			OffsetType sizeOfEntryInNeighbor = getEntrySize(LeafNodeType, firstBorrowedEntry, false);
			OffsetType finalNodeSize = (*node)->nodeSize + (sizeOfEntryInNeighbor + sizeof(OffsetType)) * sameKeyCount; //key + (leftpointer + slotOffset) * sameKeyCount
			if (finalNodeSize > PAGE_SIZE)
			{
#ifdef DEBUG
				cout << "The finalNodeSize = " << finalNodeSize << " is too large when redistributing LeafNode neighbor and node" << endl;
#endif
				return 0;
			}

			//If the entry that we decide to update in parent node is too big, we cannot and do not need to redistribute either
			OffsetType sizeOfKeyInParent = getKeySize(((LeafNode*)*node)->leafEntries[0].key);
			OffsetType finalParentSize = ((InternalNode*)*(*node)->parentPointer)->nodeSize + sizeOfKeyInParent - getKeySize(((InternalNode*)*(*node)->parentPointer)->internalEntries[keyIndex].key);
			if (finalParentSize > PAGE_SIZE)
			{
#ifdef DEBUG
				cout << "The finalParentSize = " << finalParentSize << " is too large when redistributing LeafNode neighbor and node" << endl;
#endif
				return 0;
			}

			//Update the middle key value in the parent node into the value of rightmost key in the neighbor node
			InternalEntry parentEntry(this->attrType, ((LeafNode*)*neighbor)->leafEntries.back().key);
			parentEntry.leftChild = ((InternalNode*)*(*node)->parentPointer)->internalEntries[keyIndex].leftChild;
			parentEntry.rightChild = ((InternalNode*)*(*node)->parentPointer)->internalEntries[keyIndex].rightChild;

			//Insert the borrowed entry, update parent node, and delete the entry in the neighbor node
			for (int i = 0; i < sameKeyCount; i++)
			{
				LeafEntry borrowedEntry(this->attrType, ((LeafNode*)*neighbor)->leafEntries.back().key, ((LeafNode*)*neighbor)->leafEntries.back().rid);
				((LeafNode*)*node)->leafEntries.insert(((LeafNode*)*node)->leafEntries.begin(), borrowedEntry);
				free(((LeafNode*)*neighbor)->leafEntries.back().key);
				((LeafNode*)*neighbor)->leafEntries.erase(((LeafNode*)*neighbor)->leafEntries.end() - 1);
			}
			((InternalNode*)*(*node)->parentPointer)->internalEntries[keyIndex] = parentEntry;
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
			{
#ifdef DEBUG
				cout << "The finalNodeSize = " << finalNodeSize << " is too large when redistributing InternalNode node and neighbor" << endl;
#endif
				return 0;
			}

			//Copy the leftmost key in the neighbor node and insert it in the end of node
			//The value of the borrowed key is the middle key value
			InternalEntry borrowedEntry(this->attrType, ((InternalNode*)*(*node)->parentPointer)->internalEntries[keyIndex].key);
			borrowedEntry.leftChild = ((InternalNode*)*node)->internalEntries.back().rightChild;
			borrowedEntry.rightChild = ((InternalNode*)*neighbor)->internalEntries[0].leftChild;
			(*borrowedEntry.rightChild)->parentPointer = node;

			//If the entry that we decide to update in parent node is too big, we cannot and do not need to redistribute either
			OffsetType sizeOfKeyInParent = getKeySize(((InternalNode*)*neighbor)->internalEntries[0].key);
			OffsetType finalParentSize = ((InternalNode*)*(*node)->parentPointer)->nodeSize + sizeOfKeyInParent - getKeySize(((InternalNode*)*(*node)->parentPointer)->internalEntries[keyIndex].key);
			if (finalParentSize > PAGE_SIZE)
			{
#ifdef DEBUG
				cout << "The finalParentSize = " << finalParentSize << " is too large when redistributing InternalNode node and neighbor" << endl;
#endif
				return 0;
			}

			//Update the middle key value in the parent node into the value of leftmost key in the neighbor node
			InternalEntry parentEntry(this->attrType, ((InternalNode*)*neighbor)->internalEntries[0].key);
			parentEntry.leftChild = ((InternalNode*)*(*node)->parentPointer)->internalEntries[keyIndex].leftChild;
			parentEntry.rightChild = ((InternalNode*)*(*node)->parentPointer)->internalEntries[keyIndex].rightChild;

			//Insert the borrowed entry, update parent node, and delete the entry in the neighbor node
			((InternalNode*)*node)->internalEntries.push_back(borrowedEntry);
			((InternalNode*)*(*node)->parentPointer)->internalEntries[keyIndex] = parentEntry;
			free(((InternalNode*)*neighbor)->internalEntries[0].key);
			((InternalNode*)*neighbor)->internalEntries.erase(((InternalNode*)*neighbor)->internalEntries.begin());
		}
		else if ((*node)->nodeType == LeafNodeType && (*neighbor)->nodeType == LeafNodeType)
		{
			//Count the leftmost same keys in the neighbor
			int sameKeyCount = 0;
			void * startKey = ((LeafNode*)*neighbor)->leafEntries[0].key;
			for (auto &i : ((LeafNode*)*neighbor)->leafEntries)
			{
				if (compareKey(startKey, i.key) == 0)
					sameKeyCount++;
				else
					break;
			}
			//If neighbor only has one kind of keys, we cannot and do not need to redistribute either
			if (sameKeyCount == (int)((LeafNode*)*neighbor)->leafEntries.size())
			{
#ifdef DEBUG
				cout << "The sameKeyCount = " << sameKeyCount << " when redistributing LeafNode node and neighbor" << endl;
#endif
				return 0;
			}

			//Copy the leftmost key in the neighbor node and insert it in the back of node
			LeafEntry firstBorrowedEntry(this->attrType, ((LeafNode*)*neighbor)->leafEntries[0].key, ((LeafNode*)*neighbor)->leafEntries[0].rid);

			//If the entry that we decide to borrow from neighbor is too big, we cannot and do not need to redistribute either
			OffsetType sizeOfEntryInNeighbor = getEntrySize(LeafNodeType, firstBorrowedEntry, false);
			OffsetType finalNodeSize = (*node)->nodeSize + (sizeOfEntryInNeighbor + sizeof(OffsetType)) * sameKeyCount; //key + (leftpointer + slotOffset) * sameKeyCount
			if (finalNodeSize > PAGE_SIZE)
			{
#ifdef DEBUG
				cout << "The finalNodeSize = " << finalNodeSize << " is too large when redistributing LeafNode node and neighbor" << endl;
#endif
				return 0;
			}

			//If the entry that we decide to update in parent node is too big, we cannot and do not need to redistribute either
			OffsetType sizeOfKeyInParent = getKeySize(((LeafNode*)*neighbor)->leafEntries[sameKeyCount].key);
			OffsetType finalParentSize = ((InternalNode*)*(*node)->parentPointer)->nodeSize + sizeOfKeyInParent - getKeySize(((InternalNode*)*(*node)->parentPointer)->internalEntries[keyIndex].key);
			if (finalParentSize > PAGE_SIZE)
			{
#ifdef DEBUG
				cout << "The finalParentSize = " << finalParentSize << " is too large when redistributing LeafNode node and neighbor" << endl;
#endif
				return 0;
			}

			//Update the middle key value in the parent node into the value of leftmost key in the neighbor node
			InternalEntry parentEntry(this->attrType, ((LeafNode*)*neighbor)->leafEntries[sameKeyCount].key);
			parentEntry.leftChild = ((InternalNode*)*(*node)->parentPointer)->internalEntries[keyIndex].leftChild;
			parentEntry.rightChild = ((InternalNode*)*(*node)->parentPointer)->internalEntries[keyIndex].rightChild;

			//Insert the borrowed entry, update parent node, and delete the entry in the neighbor node
			for (int i = 0; i < sameKeyCount; i++)
			{
				LeafEntry borrowedEntry(this->attrType, ((LeafNode*)*neighbor)->leafEntries[i].key, ((LeafNode*)*neighbor)->leafEntries[i].rid);
				((LeafNode*)*node)->leafEntries.push_back(borrowedEntry);
				free(((LeafNode*)*neighbor)->leafEntries[0].key);
				((LeafNode*)*neighbor)->leafEntries.erase(((LeafNode*)*neighbor)->leafEntries.begin());
			}
			((InternalNode*)*(*node)->parentPointer)->internalEntries[keyIndex] = parentEntry;
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

	if ((*node)->nodeType == InternalNodeType)
	{
		OffsetType size = sizeof(MarkType) + sizeof(OffsetType) + sizeof(PageNum); //type + usedspace + parent pointer
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
	else if ((*node)->nodeType == LeafNodeType)
	{
		OffsetType size = sizeof(MarkType) + sizeof(OffsetType) + sizeof(PageNum) * 3; //type + usedspace + parent pointer + right pointer + overflow pointer
		for (auto &i : ((LeafNode*)*node)->leafEntries)
		{
			size += getKeySize(i.key); //key
			size += sizeof(PageNum) + sizeof(unsigned); //Rid
		}
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
		if (i.second && *i.second && (*i.second)->isDirty && (*i.second)->isLoaded)
		{
			char* pageData = generatePage(i.second);
			if (ixfileHandle.writePage(i.first, pageData) == -1)
			{
#ifdef DEBUG
				cerr << "Cannot write the node back, pageNum = " << (*i.second)->pageNum << endl;
#endif
				free(pageData);
				return -1;
			}
			(*i.second)->isDirty = false;
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
	if (getNodesMergeSize(node, neighbor, keySize, sizeOfMergeNodes) == -1)
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
		int parentPageNum = (*(*node)->parentPointer)->pageNum;
		memcpy(pageData + offset, &parentPageNum, sizeof(int));
		offset += sizeof(int);
	}
	else
	{
		int parentPageNum = NULLNODE;
		memcpy(pageData + offset, &parentPageNum, sizeof(int));
		offset += sizeof(int);
	}
	if ((*node)->nodeType == InternalNodeType)
	{
		OffsetType slotCount = (OffsetType)((const InternalNode*)*node)->internalEntries.size();
		memcpy(pageData + PAGE_SIZE - sizeof(OffsetType), &slotCount, sizeof(OffsetType));
		for (OffsetType i = 0; i < slotCount; i++)
		{
			//If the slot is not the last slot, we only write its left child in page
			int leftChildPageNum = (*((const InternalNode*)*node)->internalEntries[i].leftChild)->pageNum;
			memcpy(pageData + offset, &leftChildPageNum, sizeof(int));
			offset += sizeof(int);
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
				int rightChildPageNum = (*((const InternalNode*)*node)->internalEntries[i].rightChild)->pageNum;
				memcpy(pageData + offset, &rightChildPageNum, sizeof(int));
				offset += sizeof(int);
			}
		}
	}
	else if ((*node)->nodeType == LeafNodeType)
	{
		//Store the pageNum of right pointer
		if (((const LeafNode*)*node)->rightPointer && *((const LeafNode*)*node)->rightPointer)
		{
			int rightPageNum = (*((const LeafNode*)*node)->rightPointer)->pageNum;
			memcpy(pageData + offset, &rightPageNum, sizeof(int));
			offset += sizeof(int);
		}
		else
		{
			int rightPageNum = NULLNODE;
			memcpy(pageData + offset, &rightPageNum, sizeof(int));
			offset += sizeof(int);
		}
		//Store the pageNum of overflow pointer
		if (((const LeafNode*)*node)->overflowPointer && *((const LeafNode*)*node)->overflowPointer)
		{
			int overflowPageNum = (*((const LeafNode*)*node)->overflowPointer)->pageNum;
			memcpy(pageData + offset, &overflowPageNum, sizeof(int));
			offset += sizeof(int);
		}
		else
		{
			int overflowPageNum = NULLNODE;
			memcpy(pageData + offset, &overflowPageNum, sizeof(int));
			offset += sizeof(int);
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

		int parent;
		memcpy(&parent, data + offset, sizeof(int));
		offset += sizeof(int);
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
			if (this->attrType == AttrType::TypeInt)
			{
				keyLength = sizeof(int);
			}
			else if (this->attrType == AttrType::TypeReal)
			{
				keyLength = sizeof(float);
			}
			else if (this->attrType == AttrType::TypeVarChar)
			{
				int strLength = *(int*)(data + slotOffset);
				keyLength = sizeof(int) + strLength;
			}

			//Generate left child pointer
			OffsetType leftChildPointerOffset = slotOffset - sizeof(int);
			int leftChildPageNum = *(int*)(data + leftChildPointerOffset);

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
			int rightChildPageNum = *(int*)(data + rightChildPointerOffset);
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

		int parent;
		memcpy(&parent, data + offset, sizeof(int));
		offset += sizeof(int);
		if (parent != NULLNODE)
			(*result)->parentPointer = this->nodeMap[parent];
		else
			(*result)->parentPointer = NULL;

		int rightPageNum;
		memcpy(&rightPageNum, data + offset, sizeof(int));
		offset += sizeof(int);
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

		int overflowPageNum;
		memcpy(&overflowPageNum, data + offset, sizeof(int));
		offset += sizeof(int);
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
			if (this->attrType == AttrType::TypeInt)
			{
				keyLength = sizeof(int);
			}
			else if (this->attrType == AttrType::TypeReal)
			{
				keyLength = sizeof(float);
			}
			else if (this->attrType == AttrType::TypeVarChar)
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

