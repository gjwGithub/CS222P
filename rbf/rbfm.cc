#include "rbfm.h"

RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = 0;

RecordBasedFileManager* RecordBasedFileManager::instance()
{
    if(!_rbf_manager)
        _rbf_manager = new RecordBasedFileManager();

    return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager()
{
}

RecordBasedFileManager::~RecordBasedFileManager()
{
}

RC RecordBasedFileManager::createFile(const string &fileName) {
    return PagedFileManager::instance()->createFile(fileName);
}

RC RecordBasedFileManager::destroyFile(const string &fileName) {
    return PagedFileManager::instance()->destroyFile(fileName);
}

RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle) {
	RC status = PagedFileManager::instance()->openFile(fileName, fileHandle);
	status |= fileHandle.generateAllPagesSize(this->allPagesSize);
    return status;
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
    return PagedFileManager::instance()->closeFile(fileHandle);
}

void RecordBasedFileManager::generateFieldInfo(const vector<Attribute> &recordDescriptor, const void *data, OffsetType &resultLength, char* &result, OffsetType &dataSize)
{
	OffsetType dataOffset = 0;
	bool nullBit = false;
	int nullFieldsIndicatorActualSize = ceil((double)recordDescriptor.size() / CHAR_BIT);
	unsigned char *nullFieldsIndicator = (unsigned char*)malloc(nullFieldsIndicatorActualSize);
	memcpy(nullFieldsIndicator, (char *)data + dataOffset, nullFieldsIndicatorActualSize);
	dataOffset += nullFieldsIndicatorActualSize;

	OffsetType fieldsNum = recordDescriptor.size();
	resultLength = sizeof(OffsetType) + fieldsNum * sizeof(OffsetType);
	result = (char*)malloc(resultLength);
	OffsetType offset = 0;
	memcpy(result + offset, &fieldsNum, sizeof(OffsetType));
	offset += sizeof(OffsetType);
	for (size_t i = 0; i < recordDescriptor.size(); i++)
	{
		OffsetType fieldOffset = 0;
		nullBit = nullFieldsIndicator[i / CHAR_BIT] & (1 << (CHAR_BIT - 1 - i % CHAR_BIT));
		if (!nullBit)
		{
			AttrType type = recordDescriptor[i].type;
			if (type == TypeInt)
			{
				fieldOffset = dataOffset;
				dataOffset += sizeof(int);
			}
			else if (type == TypeReal)
			{
				fieldOffset = dataOffset;
				dataOffset += sizeof(float);
			}
			else if (type == TypeVarChar)
			{
				fieldOffset = dataOffset;
				int strLength;
				memcpy(&strLength, (char *)data + dataOffset, sizeof(int));
				dataOffset += sizeof(int);
				dataOffset += strLength;
			}
		}
		else
			fieldOffset = -1;
		if (fieldOffset != -1)
			fieldOffset += sizeof(OffsetType) + resultLength - nullFieldsIndicatorActualSize;
		memcpy(result + offset, &fieldOffset, sizeof(OffsetType));
		offset += sizeof(OffsetType);
	}
	free(nullFieldsIndicator);
	dataSize = dataOffset - nullFieldsIndicatorActualSize;
}

RC RecordBasedFileManager::appendDataInPage(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, int pageNum, const void *data, RID &rid, OffsetType &fieldInfoSize, char* &fieldInfo, OffsetType &dataSize)
{
	OffsetType oldPageSize = this->allPagesSize[pageNum];
	OffsetType slotSize = sizeof(OffsetType) + fieldInfoSize + dataSize; // slotSizeNumber + fieldInfoSize + dataSize
	OffsetType newPageSize = oldPageSize + slotSize + sizeof(OffsetType);
    if (newPageSize <= PAGE_SIZE)
    {
		char *pageData = (char*)malloc(PAGE_SIZE);
		RC status = fileHandle.readPage(pageNum, pageData);
		if (status == -1)
		{
#ifdef DEBUG
			cerr << "Cannot read page " << pageNum << " while append data in that page" << endl;
#endif
			free(pageData);
			return -1;
		}
		OffsetType slotCount;
		memcpy(&slotCount, pageData + PAGE_SIZE - sizeof(OffsetType), sizeof(OffsetType));
		OffsetType newSlotTableSize = generateSlotTable(pageData, slotCount);
		OffsetType oldSlotTableSize = newSlotTableSize - sizeof(OffsetType);
		this->allPagesSize[pageNum] = newPageSize;
		int nullFieldsIndicatorActualSize = ceil((double)recordDescriptor.size() / CHAR_BIT);
		OffsetType offset = 0;
        memcpy(pageData + offset, &newPageSize, sizeof(OffsetType));
        offset += oldPageSize - oldSlotTableSize;
        memcpy(pageData + offset, &slotSize, sizeof(OffsetType));
        offset += sizeof(OffsetType);
		memcpy(pageData + offset, fieldInfo, fieldInfoSize);
		offset += fieldInfoSize;
        memcpy(pageData + offset, (char *)data + nullFieldsIndicatorActualSize, dataSize);
        status = fileHandle.writePage(pageNum, pageData);
        if (status == -1)
        {
#ifdef DEBUG
            cerr << "Cannot write page " << pageNum << endl;
#endif
			this->allPagesSize[pageNum] = oldPageSize;
			free(pageData);
            return -1;
        }
        rid.pageNum = pageNum;
        rid.slotNum = slotCount;
		free(pageData);
    }
    else
    {
        return 0;
    }
    return 1;
}

RC RecordBasedFileManager::addDataInNewPage(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid, OffsetType &fieldInfoSize, char* &fieldInfo, OffsetType &dataSize)
{
	OffsetType slotSize = sizeof(OffsetType) + fieldInfoSize + dataSize;
	int nullFieldsIndicatorActualSize = ceil((double)recordDescriptor.size() / CHAR_BIT);
	char *newData = (char*)calloc(PAGE_SIZE, 1);
	OffsetType slotCount = 0;
	OffsetType slotTableSize = generateSlotTable(newData, slotCount);
	OffsetType pageSize = sizeof(OffsetType) + slotSize + slotTableSize;
	this->allPagesSize.push_back(pageSize);
	OffsetType offset = 0;
    memcpy(newData + offset, &pageSize, sizeof(OffsetType));
    offset += sizeof(OffsetType);
    memcpy(newData + offset, &slotSize, sizeof(OffsetType));
    offset += sizeof(OffsetType);
	memcpy(newData + offset, fieldInfo, fieldInfoSize);
	offset += fieldInfoSize;
	memcpy(newData + offset, (char *)data + nullFieldsIndicatorActualSize, dataSize);
    RC status = fileHandle.appendPage(newData);
    if (status == -1)
    {
#ifdef DEBUG
        cerr << "Cannot append a page while insert a record" << endl;
#endif
		this->allPagesSize.pop_back();
		free(newData);
        return -1;
    }
	int totalPageNum = fileHandle.getNumberOfPages();
	rid.pageNum = totalPageNum - 1;
	rid.slotNum = 0;
    free(newData);
    return 0;
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid) {
    int totalPageNum = fileHandle.getNumberOfPages();
	OffsetType dataSize;
	OffsetType fieldInfoSize;
	char* fieldInfo;
	generateFieldInfo(recordDescriptor, data, fieldInfoSize, fieldInfo, dataSize);
    if (totalPageNum == 0)
    {
        RC status = addDataInNewPage(fileHandle, recordDescriptor, data, rid, fieldInfoSize, fieldInfo, dataSize);
		if (status == -1)
		{
#ifdef DEBUG
			cerr << "Cannot append a page while insert a record and PageNum = 0" << endl;
#endif
			free(fieldInfo);
			return -1;
		}
    }
    else
    {
        RC status = appendDataInPage(fileHandle, recordDescriptor, totalPageNum - 1, data, rid, fieldInfoSize, fieldInfo, dataSize);
		if (status == -1)
		{
#ifdef DEBUG
			cerr << "Cannot add data in the last page while insert a record" << endl;
#endif
			free(fieldInfo);
			return -1;
		}
        else if (status == 0)
        {
            for (int i = 0; i < totalPageNum - 1; i++)
            {
                status = appendDataInPage(fileHandle, recordDescriptor, i, data, rid, fieldInfoSize, fieldInfo, dataSize);
				if (status == -1)
				{
#ifdef DEBUG
					cerr << "Cannot add data in page " << i << " while insert a record" << endl;
#endif
					free(fieldInfo);
					return -1;
				}
				else if (status == 1)
				{
					free(fieldInfo);
					return 0;
				}
            }
			status = addDataInNewPage(fileHandle, recordDescriptor, data, rid, fieldInfoSize, fieldInfo, dataSize);
			if (status == -1)
			{
#ifdef DEBUG
				cerr << "Cannot append a page while insert a record and all the other pages are full" << endl;
#endif
				free(fieldInfo);
				return -1;
			}
			free(fieldInfo);
			return 0;
        }  
    }
	free(fieldInfo);
    return 0;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
	PageNum pageNum = rid.pageNum;
	unsigned int slotNum = rid.slotNum;
	char* pageData = (char*)malloc(PAGE_SIZE);
	RC status = fileHandle.readPage(pageNum, pageData);
	if (status == -1)
	{
#ifdef DEBUG
		cerr << "Cannot read page " << pageNum << endl;
#endif
		free(pageData);
		return -1;
	}
	
	OffsetType slotCount;
	memcpy(&slotCount, pageData + PAGE_SIZE - sizeof(OffsetType), sizeof(OffsetType));
	if ((OffsetType)slotNum >= slotCount)
	{
#ifdef DEBUG
		cerr << "SlotNum " << slotNum << " in RID is larger than the number of slots in this page!" << endl;
#endif
		free(pageData);
		return -1;
	}
	OffsetType offset;
	memcpy(&offset, pageData + PAGE_SIZE - sizeof(OffsetType) * (slotNum + 2), sizeof(OffsetType));
	OffsetType startOffset = offset;

	OffsetType slotSize;
	memcpy(&slotSize, pageData + offset, sizeof(OffsetType));
	offset += sizeof(OffsetType);
	OffsetType fieldNum;
	memcpy(&fieldNum, pageData + offset, sizeof(OffsetType));
	offset += sizeof(OffsetType);
	int nullFieldsIndicatorActualSize = ceil((double)recordDescriptor.size() / CHAR_BIT);
	unsigned char *nullFieldsIndicator = (unsigned char*)malloc(nullFieldsIndicatorActualSize);
	for (OffsetType i = 0; i < fieldNum; i += 8)
	{
		unsigned char nullFields = 0;
		for (int j = 0; i + j < fieldNum && j < 8; j++)
		{
			OffsetType fieldOffset;
			memcpy(&fieldOffset, pageData + offset, sizeof(OffsetType));
			offset += sizeof(OffsetType);
			if (fieldOffset == -1)
				nullFields += 1 << (7 - j);
		}
		nullFieldsIndicator[i / 8] = nullFields;
	}
	memcpy(data, nullFieldsIndicator, nullFieldsIndicatorActualSize);
	OffsetType dataSize = slotSize - (offset - startOffset);
	memcpy((char*)data + nullFieldsIndicatorActualSize, pageData + offset, dataSize);
	free(pageData);
	free(nullFieldsIndicator);
	return 0;
}

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) {
	OffsetType offset = 0;
	bool nullBit = false;
	int nullFieldsIndicatorActualSize = ceil((double)recordDescriptor.size() / CHAR_BIT);
	unsigned char *nullFieldsIndicator = (unsigned char*)malloc(nullFieldsIndicatorActualSize);
	memcpy(nullFieldsIndicator, (char *)data + offset, nullFieldsIndicatorActualSize);
	offset += nullFieldsIndicatorActualSize;
	for (size_t i = 0; i < recordDescriptor.size(); i++)
	{
		cout << recordDescriptor[i].name << ": ";
		nullBit = nullFieldsIndicator[i / CHAR_BIT] & (1 << (CHAR_BIT - 1 - i % CHAR_BIT));
		if (!nullBit)
		{
			AttrType type = recordDescriptor[i].type;
			if (type == TypeInt)
			{
				int intValue;
				memcpy(&intValue, (char *)data + offset, sizeof(int));
				offset += sizeof(int);
				cout << intValue << "\t";
			}
			else if (type == TypeReal)
			{
				float floatValue;
				memcpy(&floatValue, (char *)data + offset, sizeof(float));
				offset += sizeof(float);
				cout << floatValue << "\t";
			}
			else if (type == TypeVarChar)
			{
				int strLength;
				memcpy(&strLength, (char *)data + offset, sizeof(int));
				offset += sizeof(int);
				char* str = (char*)malloc(strLength + 1);
				str[strLength] = '\0';
				memcpy(str, (char *)data + offset, strLength);
				offset += strLength;
				cout << str << "\t";
				free(str);
			}
		}
		else
			cout << "Null\t";
	}
	cout << endl;
	free(nullFieldsIndicator);
	return 0;
}

OffsetType RecordBasedFileManager::generateSlotTable(char* &data, OffsetType &slotCount)
{
	OffsetType newSlotCount = slotCount + 1;
	OffsetType slotTableSize = 0;
	if (slotCount == 0)
	{
		memcpy(data + PAGE_SIZE - sizeof(OffsetType), &newSlotCount, sizeof(OffsetType));
		OffsetType firstSlotOffset = sizeof(OffsetType);
		memcpy(data + PAGE_SIZE - sizeof(OffsetType) * (newSlotCount + 1), &firstSlotOffset, sizeof(OffsetType));
	}
	else
	{
		OffsetType lastSlotOffset;
		memcpy(&lastSlotOffset, data + PAGE_SIZE - sizeof(OffsetType) * (slotCount + 1), sizeof(OffsetType));
		OffsetType lastSlotSize;
		memcpy(&lastSlotSize, data + lastSlotOffset, sizeof(OffsetType));
		OffsetType newSlotOffset = lastSlotOffset + lastSlotSize;
		memcpy(data + PAGE_SIZE - sizeof(OffsetType), &newSlotCount, sizeof(OffsetType));
		memcpy(data + PAGE_SIZE - sizeof(OffsetType) * (newSlotCount + 1), &newSlotOffset, sizeof(OffsetType));
	}
	slotTableSize = sizeof(OffsetType) * (newSlotCount + 1);
	return slotTableSize;
}

RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid)
{
	PageNum pageNum = rid.pageNum;
	unsigned int slotNum = rid.slotNum;
	char* pageData = (char*)malloc(PAGE_SIZE);
	fileHandle.readPage(pageNum, pageData);
	//Get the number of slots
	OffsetType slotCount;
	memcpy(&slotCount, pageData + PAGE_SIZE - sizeof(OffsetType), sizeof(OffsetType));
	if ((OffsetType)slotNum >= slotCount)
	{
#ifdef DEBUG
		cerr << "SlotNum " << slotNum << " in RID is larger than the number of slots in this page!" << endl;
#endif
		free(pageData);
		return -1;
	}
	//Decrease the number of slots by 1
	--slotCount;
	memcpy(pageData + PAGE_SIZE - sizeof(OffsetType), &slotCount, sizeof(OffsetType));
	//Get offset and size of target slot
	OffsetType slotOffset;
	memcpy(&slotOffset, pageData + PAGE_SIZE - sizeof(OffsetType) * (slotNum + 2), sizeof(OffsetType));
	OffsetType slotSize;
	memcpy(&slotSize, pageData + slotOffset, sizeof(OffsetType));
	//Decrease the size of page by target slot size
	OffsetType pageSize = this->allPagesSize[pageNum];
	pageSize -= slotSize;
	memcpy(pageData, &pageSize, sizeof(OffsetType));
	this->allPagesSize[pageNum] = pageSize;
	//Move forward other slots if target slot is not the last one
	if (slotNum < slotCount)
		moveSlots(slotOffset, slotNum + 1, slotCount, pageData);
	//Change the target slot offset into -1
	slotOffset = -1;
	memcpy(pageData + PAGE_SIZE - sizeof(OffsetType) * (slotNum + 2), &slotOffset, sizeof(OffsetType));
	fileHandle.writePage(pageNum, pageData);
	free(pageData);
	return 0;
}

void RecordBasedFileManager::moveSlots(const OffsetType targetOffset, const OffsetType startSlot, const OffsetType endSlot, char* &pageData)
{
	OffsetType slotCount;
	memcpy(&slotCount, pageData + PAGE_SIZE - sizeof(OffsetType), sizeof(OffsetType));
	if (startSlot > slotCount - 1)
		return;
	OffsetType startOffset;
	memcpy(&startOffset, pageData + PAGE_SIZE - sizeof(OffsetType) * (startSlot + 2), sizeof(OffsetType));
	OffsetType endOffset;
	memcpy(&endOffset, pageData + PAGE_SIZE - sizeof(OffsetType) * (endSlot + 2), sizeof(OffsetType));
	OffsetType endSlotSize;
	memcpy(&endSlotSize, pageData + endOffset, sizeof(OffsetType));
	endOffset += endSlotSize;

	OffsetType moveLength = endOffset - startOffset;
	memcpy(pageData + targetOffset, pageData + startOffset, moveLength);
	OffsetType deltaOffset = targetOffset - startOffset;
	for (OffsetType i = startSlot; i <= endSlot; i++)
	{
		OffsetType currentSlotOffset;
		memcpy(&currentSlotOffset, pageData + PAGE_SIZE - sizeof(OffsetType) * (i + 2), sizeof(OffsetType));
		currentSlotOffset += deltaOffset;
		memcpy(pageData + PAGE_SIZE - sizeof(OffsetType) * (i + 2), &currentSlotOffset, sizeof(OffsetType));
	}
}

RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid)
{
	OffsetType dataSize;
	OffsetType fieldInfoSize;
	char* fieldInfo;
	generateFieldInfo(recordDescriptor, data, fieldInfoSize, fieldInfo, dataSize);

	PageNum pageNum = rid.pageNum;
	unsigned int slotNum = rid.slotNum;
	char* pageData = (char*)malloc(PAGE_SIZE);
	fileHandle.readPage(pageNum, pageData);
	OffsetType slotOffset;
	memcpy(&slotOffset, pageData + PAGE_SIZE - sizeof(OffsetType) * (slotNum + 2), sizeof(OffsetType));
	OffsetType oldSlotSize;
	memcpy(&oldSlotSize, pageData + slotOffset, sizeof(OffsetType));
	OffsetType newSlotSize = sizeof(OffsetType) + fieldInfoSize + dataSize;

	if (oldSlotSize == newSlotSize)
	{
		OffsetType offset = slotOffset + sizeof(OffsetType);
		memcpy(pageData + offset, fieldInfo, fieldInfoSize);
		offset += fieldInfoSize;
		int nullFieldsIndicatorActualSize = ceil((double)recordDescriptor.size() / CHAR_BIT);
		memcpy(pageData + offset, (char *)data + nullFieldsIndicatorActualSize, dataSize);
	}
	else if (oldSlotSize > newSlotSize)
	{
		OffsetType offset = slotOffset;
		memcpy(pageData + offset, &newSlotSize, sizeof(OffsetType));
		offset += sizeof(OffsetType);
		memcpy(pageData + offset, fieldInfo, fieldInfoSize);
		offset += fieldInfoSize;
		int nullFieldsIndicatorActualSize = ceil((double)recordDescriptor.size() / CHAR_BIT);
		memcpy(pageData + offset, (char *)data + nullFieldsIndicatorActualSize, dataSize);
		offset += dataSize;
		OffsetType slotCount;
		memcpy(&slotCount, pageData + PAGE_SIZE - sizeof(OffsetType), sizeof(OffsetType));
		moveSlots(offset, slotNum + 1, slotCount, pageData);
	}
	else
	{
		OffsetType newPageSize = this->allPagesSize[pageNum] + newSlotSize - oldSlotSize;
		if (newPageSize <= PAGE_SIZE)
		{
			OffsetType slotEndOffset = slotOffset + newSlotSize;
			OffsetType slotCount;
			memcpy(&slotCount, pageData + PAGE_SIZE - sizeof(OffsetType), sizeof(OffsetType));
			moveSlots(slotEndOffset, slotNum + 1, slotCount, pageData);
			OffsetType offset = slotOffset;
			memcpy(pageData + offset, &newSlotSize, sizeof(OffsetType));
			offset += sizeof(OffsetType);
			memcpy(pageData + offset, fieldInfo, fieldInfoSize);
			offset += fieldInfoSize;
			int nullFieldsIndicatorActualSize = ceil((double)recordDescriptor.size() / CHAR_BIT);
			memcpy(pageData + offset, (char *)data + nullFieldsIndicatorActualSize, dataSize);
		}
		else
		{
			RID newRid;
			RC status = insertRecord(fileHandle, recordDescriptor, data, newRid);
			if (status == -1)
			{
#ifdef DEBUG
				cerr << "Cannot insert a record into another page when updating a record exceeds the size of page" << endl;
#endif
				free(pageData);
				return -1;
			}
			OffsetType offset = slotOffset;
			OffsetType tombStoneMark = -1;
			memcpy(pageData + offset, &tombStoneMark, sizeof(OffsetType));
			offset += sizeof(OffsetType);
			memcpy(pageData + offset, &(newRid.pageNum), sizeof(PageNum));
			offset += sizeof(PageNum);
			memcpy(pageData + offset, &(newRid.slotNum), sizeof(PageNum));
			offset += sizeof(OffsetType);
			OffsetType slotCount;
			memcpy(&slotCount, pageData + PAGE_SIZE - sizeof(OffsetType), sizeof(OffsetType));
			moveSlots(offset, slotNum + 1, slotCount, pageData);
		}
	}
	fileHandle.writePage(pageNum, pageData);
	free(pageData);
	return 0;
}
