
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
    RC status=PagedFileManager::instance()->createFile(fileName);
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
	unsigned ixReadPageCounter=0;
    unsigned ixWritePageCounter=0;
    unsigned ixAppendPageCounter=0;
    int root=-1;
    int smallestLeaf=-1;
    char* metaData = (char*)calloc(PAGE_SIZE, 1);
    OffsetType offset = 0;
    memcpy(metaData+offset,&ixReadPageCounter,sizeof(unsigned));
    offset+=sizeof(unsigned);
    memcpy(metaData+offset,&ixWritePageCounter,sizeof(unsigned));
    offset+=sizeof(unsigned);
    memcpy(metaData+offset,&ixAppendPageCounter,sizeof(unsigned));
    offset+=sizeof(unsigned);
    memcpy(metaData+offset,&root,sizeof(int));
    offset+=sizeof(int);
    memcpy(metaData+offset,&smallestLeaf,sizeof(int));
    offset+=sizeof(int);
    size_t writeSize=fwrite(metaData,1,PAGE_SIZE,file);
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
RC IXFileHandle::readMetaPage(){
	if(!handle.file){
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
	free(data);
	return 0;
}
RC IXFileHandle::writeMetaPage(){
	if(!handle.file){
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
	char* data = (char*)calloc(PAGE_SIZE,1);
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
    return PagedFileManager::instance()->openFile(fileName, ixfileHandle.handle);
}

RC IndexManager::closeFile(IXFileHandle &ixfileHandle)
{
    return PagedFileManager::instance()->closeFile(ixfileHandle.handle);
}

RC IndexManager::insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
	LeafEntry pair(attribute,key,rid);
    if(tree==NULL){
    	tree=new BTree();
    	tree->attrType=attribute.type;
    }
    	RC rel=tree->insertEntry(ixfileHandle,pair);
    	return rel;
    
}

RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
    return -1;
}

RC IXFileHandle::readPage(PageNum pageNum, void *data){
	ixReadPageCounter++;
	RC rel=handle.readPage(pageNum,data);
	return rel;
}

RC IXFileHandle::writePage(PageNum pageNum, const void *data){
	ixWritePageCounter++;
	RC rel=handle.writePage(pageNum,data);
	return rel;
}

RC IXFileHandle::appendPage(const void *data){
	ixAppendPageCounter++;
	RC rel=handle.appendPage(data);
	return rel;
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

LeafEntry::LeafEntry(const Attribute &attribute, const void* key,const RID rid)
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

InternalEntry::InternalEntry(const Attribute &attribute,const void* key, Node** leftChild,Node** rightChild)
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
	this->leftChild = leftChild;
	this->rightChild = rightChild;
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
	int entry_length;
		if (attrType == TypeVarChar) {
			int var_length;
			memcpy(&var_length, (char *)pair.key, 4);
			entry_length = 4 + var_length + 8;
		}
		else {
			entry_length = 12;
		}
	if (*root == NULL) {  //insert into the first node in the new tree
		//set node fields
		Node** new_node = new Node*;
		*new_node = new LeafNode();
		((LeafNode*)*new_node)->leafEntries.push_back(pair);
		((LeafNode*)*new_node)->nodeType = LeafNodeType;
		((LeafNode*)*new_node)->nodeSize += entry_length+sizeof(OffsetType);
		((LeafNode*)*new_node)->isLoaded=true;
		//set tree fields
		root = new_node;
		smallestLeaf = new_node;

		//write to the file
		void* new_page =malloc(PAGE_SIZE);
		new_page= generatePage(new_node);
		ixfileHandle.appendPage(new_page);
		nodeMap.insert(make_pair(ixfileHandle.ixAppendPageCounter-1,new_node));
		((LeafNode*)*new_node)->pageNum=ixfileHandle.ixAppendPageCounter-1;
		//set meta fields
		ixfileHandle.root=ixfileHandle.ixAppendPageCounter-1;
		ixfileHandle.smallestLeaf=ixfileHandle.ixAppendPageCounter-1;
		ixfileHandle.writeMetaPage();
	}else{
		Node** root_node;  // Initiat root node
		unordered_map<PageNum, Node**>::iterator iter; //judge the curnode exist in the hashmap
		iter=nodeMap.find(ixfileHandle.root);
		if(iter!=nodeMap.end()){
			root_node=iter->second;
		}else{
			void* data=malloc(PAGE_SIZE);
			ixfileHandle.readPage(ixfileHandle.root,data);
			root_node=generateNode((char *)data);
			nodeMap.insert(make_pair(ixfileHandle.root,root_node));
		}
		Node** tar_leafNode=findLeafnode(ixfileHandle,pair,root_node);     //find the inserted leaf node
		if((*(LeafNode**)tar_leafNode)->isLoaded==false){
			void* data=malloc(PAGE_SIZE);
			PageNum cur_pageNum=(*(LeafNode**)tar_leafNode)->pageNum;
			ixfileHandle.readPage(cur_pageNum,data);
			tar_leafNode=generateNode((char *)data);
			nodeMap.insert(make_pair(cur_pageNum,tar_leafNode));
		}
		//first insert the entry to the origin leafnode
		vector<LeafEntry>::iterator iter_vector=(*(LeafNode**)tar_leafNode)->leafEntries.begin();   //find the right position in leafnode
 		int isadded=0;         
 		for(int i=0;i<(*(LeafNode**)tar_leafNode)->leafEntries.size();i++){
 			if(compare(pair.key,(*(LeafNode**)tar_leafNode)->leafEntries[i].key)){
 				(*(LeafNode**)tar_leafNode)->leafEntries.insert(iter_vector+i,pair);
 				isadded=1;
 			}
 		}
 		if(isadded==0)
 			(*(LeafNode**)tar_leafNode)->leafEntries.push_back(pair);
 		(*(LeafNode**)tar_leafNode)->nodeSize += entry_length+sizeof(OffsetType);
 		if((*(LeafNode**)tar_leafNode)->nodeSize <=PAGE_SIZE){   //insert into node into space-enough node
 			//write the updated origin leaf node into file
 			void* new_page =malloc(PAGE_SIZE);
			new_page= generatePage(tar_leafNode);
			ixfileHandle.writePage((*(LeafNode**)tar_leafNode)->pageNum,new_page);
			free(new_page);
 		}else{                                                        //split node;
 			Node** new_node = new Node*;
			*new_node = new LeafNode();
			((LeafNode*)*new_node)->isLoaded=true;
			//move the origin leafnode the second half records to the new leafnode, and update the nodesize
			for(int i=(*(LeafNode**)tar_leafNode)->leafEntries.size()/2;i<(*(LeafNode**)tar_leafNode)->leafEntries.size();i++){
				((LeafNode*)*new_node)->leafEntries.push_back((*(LeafNode**)tar_leafNode)->leafEntries[i]);
				int var_length;
				if(attrType==TypeVarChar){
					memcpy(&var_length, (char *)((LeafNode*)*new_node)->leafEntries[i].key, 4);
					var_length += 12;
				}else{
					var_length =12;
				}
				(*(LeafNode**)tar_leafNode)->nodeSize -= var_length+sizeof(OffsetType);
				((LeafNode*)*new_node)->nodeSize += var_length+sizeof(OffsetType);
			}
			//the mid_node key's length
			int mid_index=(*(LeafNode**)tar_leafNode)->leafEntries.size()/2;
			int var_length;
			void* key_mid;
			if(attrType==TypeVarChar){
				memcpy(&var_length, (char *)((LeafNode*)*new_node)->leafEntries[mid_index].key, 4);
				key_mid=malloc(sizeof(int)+var_length);
				memcpy((char *)key_mid,(char *)((LeafNode*)*new_node)->leafEntries[mid_index].key,sizeof(int)+var_length);
			}else{
				var_length =4;
				key_mid=malloc(sizeof(int));
				memcpy((char *)key_mid,(char *)((LeafNode*)*new_node)->leafEntries[mid_index].key,sizeof(int));
			}
			//delete the second half records from the origin leafnode.
			for(int i=mid_index;i<(*(LeafNode**)tar_leafNode)->leafEntries.size();i++){
				(*(LeafNode**)tar_leafNode)->leafEntries.erase((*(LeafNode**)tar_leafNode)->leafEntries.begin()+mid_index);
			}
			//update the origin and new node's rightpointer
			((LeafNode*)*new_node)->rightPointer=(*(LeafNode**)tar_leafNode)->rightPointer;
			(*(LeafNode**)tar_leafNode)->rightPointer=new_node;
			((LeafNode*)*new_node)->nodeType = LeafNodeType;

			//if it dont have the parentNode,create a parentNode
			if(*((*(LeafNode**)tar_leafNode)->parentPointer)==NULL){
				((LeafNode*)*new_node)->pageNum = ixfileHandle.ixAppendPageCounter+1;  //assume the new leaf node will append in ixappendpageCounter th
				InternalEntry internal_pair(attrType,((LeafNode*)*new_node)->leafEntries[0].key,tar_leafNode,new_node);
				Node** new_parentNode = new Node*;
				*new_parentNode = new InternalNode();
				((InternalNode*)*new_parentNode)->isLoaded=true;
				((InternalNode*)*new_parentNode)->internalEntries.push_back(internal_pair);
				((InternalNode*)*new_parentNode)->nodeType = InternalNodeType;
				((InternalNode*)*new_parentNode)->nodeSize += var_length+ 2*sizeof(int)+ sizeof(OffsetType);
				//write the father node to the file
				void* new_page=malloc(PAGE_SIZE);
				new_page = generatePage(new_parentNode);
				ixfileHandle.appendPage(new_page);
				nodeMap.insert(make_pair(ixfileHandle.ixAppendPageCounter-1,new_parentNode));
				((InternalNode*)*new_parentNode)->pageNum=ixfileHandle.ixAppendPageCounter-1;
				free(new_page);
				//set father-son relatoinship.
				(*(LeafNode**)tar_leafNode)->parentPointer=new_parentNode;
				((LeafNode*)*new_node)->parentPointer=new_parentNode;
				//set meta fields
				ixfileHandle.root=ixfileHandle.ixAppendPageCounter-1;
				ixfileHandle.writeMetaPage();
				//update the origin leafnode to the file
				void* new_originLeafpage=malloc(PAGE_SIZE);
				new_originLeafpage = generatePage(tar_leafNode);
				int originLeafpage=(*(LeafNode**)tar_leafNode)->pageNum;
				ixfileHandle.writePage(originLeafpage,new_originLeafpage);
				free(new_originLeafpage);
				//add the updated leafnode to the file
				void* new_newLeafpage =malloc(PAGE_SIZE);
				new_newLeafpage= generatePage(new_node);
				ixfileHandle.appendPage(new_newLeafpage);
				nodeMap.insert(make_pair(ixfileHandle.ixAppendPageCounter-1,new_node));
				((LeafNode*)*new_node)->pageNum=ixfileHandle.ixAppendPageCounter-1;
				free(new_newLeafpage);
			}else{  // it have the parentNode
				Node** origin_parentNode = new Node*;
				PageNum origin_parentNode_pagenum=((InternalNode*)*((*(LeafNode**)tar_leafNode)->parentPointer))->pageNum;
				iter=nodeMap.find(origin_parentNode_pagenum);
				if(iter!=nodeMap.end()){
					origin_parentNode=iter->second;
				}else{
					cout<<"not found"<<endl;
				}
				//update the origin leafnode to the file
				void* new_originLeafpage=malloc(PAGE_SIZE);
				new_originLeafpage = generatePage(tar_leafNode);
				int originLeafpage=(*(LeafNode**)tar_leafNode)->pageNum;
				ixfileHandle.writePage(originLeafpage,new_originLeafpage);
				free(new_originLeafpage);
				//add the updated leafnode to the file
				((LeafNode*)*new_node)->parentPointer=origin_parentNode;
				void* new_newLeafpage =malloc(PAGE_SIZE);
				new_newLeafpage= generatePage(new_node);
				ixfileHandle.appendPage(new_newLeafpage);
				nodeMap.insert(make_pair(ixfileHandle.ixAppendPageCounter-1,new_node));
				((LeafNode*)*new_node)->pageNum=ixfileHandle.ixAppendPageCounter-1;
				free(new_newLeafpage);

				InternalEntry internal_pair(attrType,((LeafNode*)*new_node)->leafEntries[0].key,tar_leafNode,new_node);
				//add the entry into to internalnode
				vector<InternalEntry>::iterator internal_iter_vector=(*(InternalNode**)origin_parentNode)->internalEntries.begin();   //find the right position in leafnode
 				int isadded1=0;
 				int added_index=0;         
 				for(int i=0;i<(*(InternalNode**)origin_parentNode)->internalEntries.size();i++){
 					if(compare(internal_pair.key,(*(InternalNode**)origin_parentNode)->internalEntries[i].key)){
 						(*(InternalNode**)origin_parentNode)->internalEntries.insert(internal_iter_vector+i,internal_pair);
 						isadded1=1;
 						added_index=i;
 						break;
 					}
 				}
 				if(isadded1==0){
 					(*(InternalNode**)origin_parentNode)->internalEntries.push_back(internal_pair);
 					(*(InternalNode**)origin_parentNode)->internalEntries[(*(InternalNode**)origin_parentNode)->internalEntries.size()-2].rightChild=internal_pair.leftChild;
 				}
 				else if(added_index=0){
 					(*(InternalNode**)origin_parentNode)->internalEntries[1].leftChild=internal_pair.rightChild;
 				}else{
 					(*(InternalNode**)origin_parentNode)->internalEntries[added_index-1].rightChild=internal_pair.leftChild;
 					(*(InternalNode**)origin_parentNode)->internalEntries[added_index+1].leftChild=internal_pair.rightChild;
 				}
 				((InternalNode*)*origin_parentNode)->nodeSize += var_length +2*sizeof(int)+sizeof(OffsetType);
				// if(((LeafNode*)*origin_parentNode)->nodeSize + var_length +2*sizeof(int)+sizeof(OffsetType)<=PAGE_SIZE){
				// }else{ //split the parentNode

				// }
				//
				if(((InternalNode*)*origin_parentNode)->nodeSize<=PAGE_SIZE){
					void* new_page =malloc(PAGE_SIZE);
					new_page= generatePage(tar_leafNode);
					ixfileHandle.writePage((*(InternalNode**)origin_parentNode)->pageNum,new_page);
					free(new_page);
				}
				while(((InternalNode*)*origin_parentNode)->nodeSize >PAGE_SIZE){
					Node** new_parentNode = new Node*;
					*new_parentNode = new InternalNode();
					((InternalNode*)*new_parentNode)->isLoaded=true;
					//move the origin leafnode the second half records to the new leafnode, and update the nodesize
					for(int i=(*(InternalNode**)origin_parentNode)->internalEntries.size()/2+1;i<(*(InternalNode**)origin_parentNode)->internalEntries.size();i++){
						((InternalNode*)*new_parentNode)->internalEntries.push_back((*(InternalNode**)origin_parentNode)->internalEntries[i]);
						if(attrType==TypeVarChar){
							memcpy(&var_length, (char *)((InternalNode*)*new_parentNode)->internalEntries[i].key, 4);
							var_length += 12;
						}else{
							var_length =12;
						}
						(*(InternalNode**)origin_parentNode)->nodeSize -= var_length+sizeof(OffsetType);
						((InternalNode*)*new_parentNode)->nodeSize += var_length+sizeof(OffsetType);
					}
					//the mid_node key's length
					int mid_index=(*(InternalNode**)origin_parentNode)->internalEntries.size()/2;
					void* key_mid;
					if(attrType==TypeVarChar){
						memcpy(&var_length, (char *)((InternalNode*)*new_parentNode)->internalEntries[mid_index].key, 4);
						key_mid=malloc(sizeof(int)+var_length);
						memcpy((char *)key_mid,(char *)((InternalNode*)*new_parentNode)->internalEntries[mid_index].key,sizeof(int)+var_length);
					}else{
						var_length =4;
						key_mid=malloc(sizeof(int));
						memcpy((char *)key_mid,(char *)((InternalNode*)*new_parentNode)->internalEntries[mid_index].key,sizeof(int));
					}
					//delete the second half records from the origin leafnode.
					for(int i=mid_index;i<(*(InternalNode**)origin_parentNode)->internalEntries.size();i++){
						(*(InternalNode**)origin_parentNode)->internalEntries.erase((*(InternalNode**)origin_parentNode)->internalEntries.begin()+mid_index);
					}
					//update the origin and new node's rightpointer
					((InternalNode*)*new_parentNode)->nodeType = InternalNodeType;
					if(*((*(InternalNode**)origin_parentNode)->parentPointer)==NULL){
						((InternalNode*)*new_parentNode)->pageNum = ixfileHandle.ixAppendPageCounter+1;  //assume the new leaf node will append in ixappendpageCounter th
						InternalEntry internal_pair(attrType,key_mid,origin_parentNode,new_parentNode);
						Node** new_grandparentNode = new Node*;
						*new_grandparentNode = new InternalNode();
						((InternalNode*)*new_grandparentNode)->isLoaded=true;
						((InternalNode*)*new_grandparentNode)->internalEntries.push_back(internal_pair);
						((InternalNode*)*new_grandparentNode)->nodeType = InternalNodeType;
						((InternalNode*)*new_grandparentNode)->nodeSize += var_length+ 2*sizeof(int)+ sizeof(OffsetType);
						//write the father node to the file
						void* new_page=malloc(PAGE_SIZE);
						new_page = generatePage(new_grandparentNode);
						ixfileHandle.appendPage(new_page);
						nodeMap.insert(make_pair(ixfileHandle.ixAppendPageCounter-1,new_grandparentNode));
						((InternalNode*)*new_parentNode)->pageNum=ixfileHandle.ixAppendPageCounter-1;
						free(new_page);
						//set father-son relatoinship.
						((InternalNode*)*origin_parentNode)->parentPointer=new_grandparentNode;
						((InternalNode*)*new_parentNode)->parentPointer=new_grandparentNode;
						//set meta fields
						ixfileHandle.root=ixfileHandle.ixAppendPageCounter-1;
						ixfileHandle.writeMetaPage();
						//update the origin leafnode to the file
						void* new_originLeafpage=malloc(PAGE_SIZE);
						new_originLeafpage = generatePage(origin_parentNode);
						int originLeafpage=((InternalNode*)*origin_parentNode)->pageNum;
						ixfileHandle.writePage(originLeafpage,new_originLeafpage);
						free(new_originLeafpage);
						//add the updated leafnode to the file
						void* new_newLeafpage =malloc(PAGE_SIZE);
						new_newLeafpage= generatePage(new_parentNode);
						ixfileHandle.appendPage(new_newLeafpage);
						nodeMap.insert(make_pair(ixfileHandle.ixAppendPageCounter-1,new_parentNode));
						((InternalNode*)*new_parentNode)->pageNum=ixfileHandle.ixAppendPageCounter-1;
						free(new_newLeafpage);
						break;
					}else{  // it have the parentNode
						Node** origin_grandparentNode = new Node*;
						PageNum origin_grandparentNode_pagenum=((InternalNode*)*(((InternalNode*)*origin_parentNode)->parentPointer))->pageNum;
						iter=nodeMap.find(origin_grandparentNode_pagenum);
						if(iter!=nodeMap.end()){
							origin_grandparentNode=iter->second;
						}else{
							cout<<"not found"<<endl;
						}
						//update the origin leafnode to the file
						void* new_originLeafpage=malloc(PAGE_SIZE);
						new_originLeafpage = generatePage(origin_parentNode);
						int originLeafpage=((InternalNode*)*origin_parentNode)->pageNum;
						ixfileHandle.writePage(originLeafpage,new_originLeafpage);
						free(new_originLeafpage);
						//add the updated leafnode to the file
						((InternalNode*)*new_parentNode)->parentPointer=origin_grandparentNode;
						void* new_newLeafpage =malloc(PAGE_SIZE);
						new_newLeafpage= generatePage(new_parentNode);
						ixfileHandle.appendPage(new_newLeafpage);
						nodeMap.insert(make_pair(ixfileHandle.ixAppendPageCounter-1,new_parentNode));
						((InternalNode*)*new_parentNode)->pageNum=ixfileHandle.ixAppendPageCounter-1;
						free(new_newLeafpage);

						InternalEntry internal_pair(attrType,key_mid,origin_parentNode,new_parentNode);
						//add the entry into to internalnode
						vector<InternalEntry>::iterator internal_iter_vector=(*(InternalNode**)origin_grandparentNode)->internalEntries.begin();   //find the right position in leafnode
 						int isadded1=0;
 						int added_index=0;         
 						for(int i=0;i<(*(InternalNode**)origin_grandparentNode)->internalEntries.size();i++){
 							if(compare(internal_pair.key,(*(InternalNode**)origin_grandparentNode)->internalEntries[i].key)){
 								(*(InternalNode**)origin_grandparentNode)->internalEntries.insert(internal_iter_vector+i,internal_pair);
 								isadded1=1;
 								added_index=i;
 								break;
 							}
 						}
 						if(isadded1==0){
 							(*(InternalNode**)origin_grandparentNode)->internalEntries.push_back(internal_pair);
 							(*(InternalNode**)origin_grandparentNode)->internalEntries[(*(InternalNode**)origin_grandparentNode)->internalEntries.size()-2].rightChild=internal_pair.leftChild;
 						}
 						else if(added_index=0){
 							(*(InternalNode**)origin_grandparentNode)->internalEntries[1].leftChild=internal_pair.rightChild;
 						}else{
 							(*(InternalNode**)origin_grandparentNode)->internalEntries[added_index-1].rightChild=internal_pair.leftChild;
 							(*(InternalNode**)origin_grandparentNode)->internalEntries[added_index+1].leftChild=internal_pair.rightChild;
 						}
 						((InternalNode*)*origin_grandparentNode)->nodeSize += var_length +2*sizeof(int)+sizeof(OffsetType);
 						if(((InternalNode*)*origin_grandparentNode)->nodeSize<=PAGE_SIZE){
							void* new_page =malloc(PAGE_SIZE);
							new_page= generatePage(origin_grandparentNode);
							ixfileHandle.writePage((*(InternalNode**)origin_grandparentNode)->pageNum,new_page);
							free(new_page);
							break;
						}
						origin_parentNode=origin_grandparentNode;
					}
				//update the origin_parentNode, internal_pair,var_legnth,
				}
			}
 		}
	}


}
Node** BTree::findLeafnode(IXFileHandle &ixfileHandle,const LeafEntry &pair,Node** cur_node){
	while((*cur_node)->nodeType!=LeafNodeType){
			int size=(*(InternalNode**)cur_node)->internalEntries.size();
			if(attrType==TypeInt){                    // key is int
				int key;
				memcpy(&key,(char *)pair.key,sizeof(int));
				for(int i=0;i<=size;i++){
					if(i==size){
						cur_node=(*(InternalNode**)cur_node)->internalEntries[size-1].rightChild;
						if((*(InternalNode**)cur_node)->isLoaded==false){
							void* data=malloc(PAGE_SIZE);
							PageNum cur_pageNum=(*(InternalNode**)cur_node)->pageNum;
							ixfileHandle.readPage(cur_pageNum,data);
							cur_node=generateNode((char *)data);
							nodeMap.insert(make_pair(cur_pageNum,cur_node));
						}
						break;
					}else{
					int sou_key;
					memcpy(&sou_key,(*(InternalNode**)cur_node)->internalEntries[i].key,sizeof(int));
					if(key<sou_key){
						cur_node=(*(InternalNode**)cur_node)->internalEntries[i].leftChild;
						if((*(InternalNode**)cur_node)->isLoaded==false){
							void* data=malloc(PAGE_SIZE);
							PageNum cur_pageNum=(*(InternalNode**)cur_node)->pageNum;
							ixfileHandle.readPage(cur_pageNum,data);
							cur_node=generateNode((char *)data);
							nodeMap.insert(make_pair(cur_pageNum,cur_node));
						}
						break;
					}
				}
				}
			}else if(attrType==TypeReal){
				float key;
				memcpy(&key,(char *)pair.key,sizeof(float));
				for(int i=0;i<=size;i++){
					if(i==size){
						cur_node=(*(InternalNode**)cur_node)->internalEntries[size-1].rightChild;
						if((*(InternalNode**)cur_node)->isLoaded==false){
							void* data=malloc(PAGE_SIZE);
							PageNum cur_pageNum=(*(InternalNode**)cur_node)->pageNum;
							ixfileHandle.readPage(cur_pageNum,data);
							cur_node=generateNode((char *)data);
							nodeMap.insert(make_pair(cur_pageNum,cur_node));
						}
						break;
					}else{
					int sou_key;
					memcpy(&sou_key,(*(InternalNode**)cur_node)->internalEntries[i].key,sizeof(int));
					if(key<sou_key){
						cur_node=(*(InternalNode**)cur_node)->internalEntries.leftChild;
						if((*(InternalNode**)cur_node)->isLoaded==false){
							void* data=malloc(PAGE_SIZE);
							PageNum cur_pageNum=(*(InternalNode**)cur_node)->pageNum;
							ixfileHandle.readPage(cur_pageNum,data);
							cur_node=generateNode((char *)data);
							nodeMap.insert(make_pair(cur_pageNum,cur_node));
						}
						break;
					}
				}
				}
			}else if(attrType==TypeVarChar){
				for(int i=0;i<=size;i++){
					if(i==size){
						cur_node=(*(InternalNode**)cur_node)->internalEntries[size-1].rightChild;
						if((*(InternalNode**)cur_node)->isLoaded==false){
							void* data=malloc(PAGE_SIZE);
							PageNum cur_pageNum=(*(InternalNode**)cur_node)->pageNum;
							ixfileHandle.readPage(cur_pageNum,data);
							cur_node=generateNode((char *)data);
							nodeMap.insert(make_pair(cur_pageNum,cur_node));
						}
						break;
					}else{
						if(compareKey(pair.key,(*(InternalNode**)cur_node)->internalEntries[i].key)<0){
							cur_node=(*(InternalNode**)cur_node)->internalEntries[i].leftChild;
							if((*(InternalNode**)cur_node)->isLoaded==false){
							void* data=malloc(PAGE_SIZE);
							PageNum cur_pageNum=(*(InternalNode**)cur_node)->pageNum;
							ixfileHandle.readPage(cur_pageNum,data);
							cur_node=generateNode((char *)data);
							nodeMap.insert(make_pair(cur_pageNum,cur_node));
						}
							break;
						}
					}
				}
			}
		}
		return cur_node;
}
char* BTree::generatePage(Node** node)
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

