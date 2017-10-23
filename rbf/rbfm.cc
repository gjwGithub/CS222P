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
	status |= fileHandle.generateAllPagesSize(fileHandle.allPagesSize);
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
			fieldOffset += sizeof(OffsetType) + 2 * sizeof(MarkType) + resultLength - nullFieldsIndicatorActualSize;
		memcpy(result + offset, &fieldOffset, sizeof(OffsetType));
		offset += sizeof(OffsetType);
	}
	free(nullFieldsIndicator);
	dataSize = dataOffset - nullFieldsIndicatorActualSize;
}

RC RecordBasedFileManager::appendDataInPage(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, int pageNum, const void *data, RID &rid, OffsetType &fieldInfoSize, char* &fieldInfo, OffsetType &dataSize)
{
	//Pretend that we read the page to get the page size
	OffsetType oldPageSize = fileHandle.allPagesSize[pageNum];
	++fileHandle.readPageCounter;

	OffsetType slotSize = sizeof(OffsetType) + 2 * sizeof(MarkType) + fieldInfoSize + dataSize; // slotSizeNumber + isUpdatedRecord + version + fieldInfoSize + dataSize
	OffsetType newPageSize = oldPageSize + slotSize + sizeof(OffsetType); // In fact newPageSize would be oldPageSize + slotSize, if we decide to reuse a previous deleted slot
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

		OffsetType oldSlotCount;
		memcpy(&oldSlotCount, pageData + PAGE_SIZE - sizeof(OffsetType), sizeof(OffsetType));
		OffsetType targetSlot = generateSlotTable(pageData, oldSlotCount, slotSize);
		OffsetType newSlotCount;
		memcpy(&newSlotCount, pageData + PAGE_SIZE - sizeof(OffsetType), sizeof(OffsetType));
		OffsetType newSlotTableSize = (newSlotCount + 1) * sizeof(OffsetType);
		OffsetType oldSlotTableSize = (oldSlotCount + 1) * sizeof(OffsetType);
		if (newSlotTableSize != oldSlotTableSize)
		{
			//If we add a new slot in the end of the table
			fileHandle.allPagesSize[pageNum] = newPageSize;
			int nullFieldsIndicatorActualSize = ceil((double)recordDescriptor.size() / CHAR_BIT);
			OffsetType offset = 0;
			memcpy(pageData + offset, &newPageSize, sizeof(OffsetType));
			offset += oldPageSize - oldSlotTableSize;
			memcpy(pageData + offset, &slotSize, sizeof(OffsetType));
			offset += sizeof(OffsetType);
			MarkType isUpdatedRecord = 0;
			memcpy(pageData + offset, &isUpdatedRecord, sizeof(MarkType));
			offset += sizeof(MarkType);
			MarkType version = fileHandle.getCurrentVersion();
			memcpy(pageData + offset, &version, sizeof(MarkType));
			offset += sizeof(MarkType);
			memcpy(pageData + offset, fieldInfo, fieldInfoSize);
			offset += fieldInfoSize;
			memcpy(pageData + offset, (char *)data + nullFieldsIndicatorActualSize, dataSize);
		}
		else
		{
			//If we reuse a previous deleted slot
			fileHandle.allPagesSize[pageNum] = newPageSize - sizeof(OffsetType); //Since we didn't change the size of slot table
			OffsetType targetSlotOffset;
			memcpy(&targetSlotOffset, pageData + PAGE_SIZE - sizeof(OffsetType) * (targetSlot + 2), sizeof(OffsetType));
			if (targetSlot < oldSlotCount - 1) // We need to move back other slots to make more space if we insert the slot in the middle
				moveSlots(targetSlotOffset + slotSize, targetSlot + 1, oldSlotCount - 1, pageData);
			OffsetType offset = targetSlotOffset;
			int nullFieldsIndicatorActualSize = ceil((double)recordDescriptor.size() / CHAR_BIT);
			memcpy(pageData + offset, &slotSize, sizeof(OffsetType));
			offset += sizeof(OffsetType);
			MarkType isUpdatedRecord = UpdatedRecordMark::Origin;
			memcpy(pageData + offset, &isUpdatedRecord, sizeof(MarkType));
			offset += sizeof(MarkType);
			MarkType version = fileHandle.getCurrentVersion();
			memcpy(pageData + offset, &version, sizeof(MarkType));
			offset += sizeof(MarkType);
			memcpy(pageData + offset, fieldInfo, fieldInfoSize);
			offset += fieldInfoSize;
			memcpy(pageData + offset, (char *)data + nullFieldsIndicatorActualSize, dataSize);
		}
        status = fileHandle.writePage(pageNum, pageData);
        if (status == -1)
        {
#ifdef DEBUG
            cerr << "Cannot write page " << pageNum << endl;
#endif
			fileHandle.allPagesSize[pageNum] = oldPageSize;
			free(pageData);
            return -1;
        }
        rid.pageNum = pageNum;
        rid.slotNum = targetSlot;
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
	OffsetType slotSize = sizeof(OffsetType) + 2 * sizeof(MarkType) + fieldInfoSize + dataSize; // slotSizeNumber + isUpdatedRecord + version + fieldInfoSize + dataSize
	int nullFieldsIndicatorActualSize = ceil((double)recordDescriptor.size() / CHAR_BIT);
	char *newData = (char*)calloc(PAGE_SIZE, 1);
	OffsetType slotCount = 0;
	OffsetType targetSlot = generateSlotTable(newData, slotCount, slotSize);
	if (targetSlot != 0)
	{
#ifdef DEBUG
		cerr << "Cannot generate slot table when add data in a new page" << endl;
#endif
		free(newData);
		return -1;
	}
	OffsetType pageSize = sizeof(OffsetType) + slotSize + sizeof(OffsetType) * 2; //pageSizeNum + slotSize + slotTableSize 
	fileHandle.allPagesSize.push_back(pageSize);
	OffsetType offset = 0;
    memcpy(newData + offset, &pageSize, sizeof(OffsetType));
    offset += sizeof(OffsetType);
    memcpy(newData + offset, &slotSize, sizeof(OffsetType));
    offset += sizeof(OffsetType);
	MarkType isUpdatedRecord = UpdatedRecordMark::Origin;
	memcpy(newData + offset, &isUpdatedRecord, sizeof(MarkType));
	offset += sizeof(MarkType);
	MarkType version = fileHandle.getCurrentVersion();
	memcpy(newData + offset, &version, sizeof(MarkType));
	offset += sizeof(MarkType);
	memcpy(newData + offset, fieldInfo, fieldInfoSize);
	offset += fieldInfoSize;
	memcpy(newData + offset, (char *)data + nullFieldsIndicatorActualSize, dataSize);
    RC status = fileHandle.appendPage(newData);
    if (status == -1)
    {
#ifdef DEBUG
        cerr << "Cannot append a page while insert a record" << endl;
#endif
		fileHandle.allPagesSize.pop_back();
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
	//Traverse tombstones and find real record page and slot
	RID finalRid;
	char* pageData;
	RC status = toFinalSlot(fileHandle, rid, finalRid, pageData);
	if (status == -1)
	{
#ifdef DEBUG
		cerr << "Cannot traverse to final slot when reading record." << endl;
#endif
		free(pageData);
		return -1;
	}
	unsigned int slotNum = finalRid.slotNum;

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
	if (offset == -1)
	{
#ifdef DEBUG
		cerr << "Reading a deleted record pageNum = " << finalRid.pageNum << " slotNum = " << slotNum << endl;
#endif
		free(pageData);
		return -1;
	}
	OffsetType startOffset = offset;

	OffsetType slotSize;
	memcpy(&slotSize, pageData + offset, sizeof(OffsetType));
	offset += sizeof(OffsetType) + sizeof(MarkType);
	MarkType version;
	memcpy(&version, pageData + offset, sizeof(MarkType));
	offset += sizeof(MarkType);
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

OffsetType RecordBasedFileManager::generateSlotTable(char* &data, OffsetType &slotCount, OffsetType &slotSize)
{
	OffsetType newSlotCount = slotCount + 1;
	if (slotCount == 0)
	{
		//If the slot table is empty
		memcpy(data + PAGE_SIZE - sizeof(OffsetType), &newSlotCount, sizeof(OffsetType));
		OffsetType firstSlotOffset = sizeof(OffsetType);
		memcpy(data + PAGE_SIZE - sizeof(OffsetType) * (newSlotCount + 1), &firstSlotOffset, sizeof(OffsetType));
	}
	else
	{
		//If the slot table is not empty
		//First find out whether there is a deleted slot in the table
		//If not, we will add the slot in the back
		for (OffsetType i = 0; i < slotCount; i++)
		{
			OffsetType slotOffset;
			memcpy(&slotOffset, data + PAGE_SIZE - sizeof(OffsetType) * (i + 2), sizeof(OffsetType));
			if (slotOffset == -1)
			{
				//If deleted slot is not the last slot in table
				if (i < slotCount - 1)
				{
					OffsetType nextSlotOffset;
					memcpy(&nextSlotOffset, data + PAGE_SIZE - sizeof(OffsetType) * (i + 3), sizeof(OffsetType));
					memcpy(data + PAGE_SIZE - sizeof(OffsetType) * (i + 2), &nextSlotOffset, sizeof(OffsetType));
				}
				else
				{
					OffsetType previousSlotOffset;
					memcpy(&previousSlotOffset, data + PAGE_SIZE - sizeof(OffsetType) * (i + 1), sizeof(OffsetType));
					OffsetType previousSlotSize;
					memcpy(&previousSlotSize, data + previousSlotOffset, sizeof(OffsetType));
					OffsetType targetSlotOffset = previousSlotOffset + previousSlotSize;
					memcpy(data + PAGE_SIZE - sizeof(OffsetType) * (i + 2), &targetSlotOffset, sizeof(OffsetType));
				}
				return i;
			}
		}

		OffsetType lastSlotOffset;
		memcpy(&lastSlotOffset, data + PAGE_SIZE - sizeof(OffsetType) * (slotCount + 1), sizeof(OffsetType));
		OffsetType lastSlotSize;
		memcpy(&lastSlotSize, data + lastSlotOffset, sizeof(OffsetType));
		OffsetType newSlotOffset = lastSlotOffset + lastSlotSize;
		memcpy(data + PAGE_SIZE - sizeof(OffsetType), &newSlotCount, sizeof(OffsetType));
		memcpy(data + PAGE_SIZE - sizeof(OffsetType) * (newSlotCount + 1), &newSlotOffset, sizeof(OffsetType));
	}
	return slotCount;
}

RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid)
{
	//Traverse tombstones and find real record page and slot
	RID finalRid;
	char* finalPage;
	RC status = toFinalSlot(fileHandle, rid, finalRid, finalPage);
	if (status == -1)
	{
#ifdef DEBUG
		cerr << "Cannot traverse to final slot when deleting record." << endl;
#endif
		free(finalPage);
		return -1;
	}
	PageNum pageNum = finalRid.pageNum;
	unsigned int slotNum = finalRid.slotNum;
	//Get the number of slots
	OffsetType slotCount;
	memcpy(&slotCount, finalPage + PAGE_SIZE - sizeof(OffsetType), sizeof(OffsetType));
	if ((OffsetType)slotNum >= slotCount)
	{
#ifdef DEBUG
		cerr << "SlotNum " << slotNum << " in RID is larger than the number of slots in this page!" << endl;
#endif
		free(finalPage);
		return -1;
	}
	//Get offset and size of target slot
	OffsetType slotOffset;
	memcpy(&slotOffset, finalPage + PAGE_SIZE - sizeof(OffsetType) * (slotNum + 2), sizeof(OffsetType));
	OffsetType slotSize;
	memcpy(&slotSize, finalPage + slotOffset, sizeof(OffsetType));
	//Decrease the size of page by target slot size
	OffsetType pageSize = fileHandle.allPagesSize[pageNum];
	pageSize -= slotSize;
	memcpy(finalPage, &pageSize, sizeof(OffsetType));
	fileHandle.allPagesSize[pageNum] = pageSize;
	//Move forward other slots if target slot is not the last one
	if ((OffsetType)slotNum < slotCount - 1)
		moveSlots(slotOffset, slotNum + 1, slotCount - 1, finalPage);
	//Change the target slot offset into -1
	slotOffset = -1;
	memcpy(finalPage + PAGE_SIZE - sizeof(OffsetType) * (slotNum + 2), &slotOffset, sizeof(OffsetType));
	status = fileHandle.writePage(pageNum, finalPage);
	if (status == -1)
	{
#ifdef DEBUG
		cerr << "Cannot write page back when deleting record." << endl;
#endif
		free(finalPage);
		return -1;
	}
	free(finalPage);
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
	memmove(pageData + targetOffset, pageData + startOffset, moveLength);
	OffsetType deltaOffset = targetOffset - startOffset;
	for (OffsetType i = startSlot; i <= endSlot; i++)
	{
		OffsetType currentSlotOffset;
		memcpy(&currentSlotOffset, pageData + PAGE_SIZE - sizeof(OffsetType) * (i + 2), sizeof(OffsetType));
		currentSlotOffset += deltaOffset;
		memcpy(pageData + PAGE_SIZE - sizeof(OffsetType) * (i + 2), &currentSlotOffset, sizeof(OffsetType));
	}

	//Change total size of the page
	OffsetType pageSize;
	memcpy(&pageSize, pageData, sizeof(OffsetType));
	pageSize += deltaOffset;
	memcpy(pageData, &pageSize, sizeof(OffsetType));
}

RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid)
{
	OffsetType dataSize;
	OffsetType fieldInfoSize;
	char* fieldInfo;
	generateFieldInfo(recordDescriptor, data, fieldInfoSize, fieldInfo, dataSize);

	RID finalRid;
	char* finalPage;
	RC status = toFinalSlot(fileHandle, rid, finalRid, finalPage);
	if (status == -1)
	{
#ifdef DEBUG
		cerr << "Cannot traverse to final slot when updating record." << endl;
#endif
		free(finalPage);
		return -1;
	}
	PageNum pageNum = finalRid.pageNum;
	unsigned int slotNum = finalRid.slotNum;

	OffsetType slotOffset;
	memcpy(&slotOffset, finalPage + PAGE_SIZE - sizeof(OffsetType) * (slotNum + 2), sizeof(OffsetType));
	OffsetType oldSlotSize;
	memcpy(&oldSlotSize, finalPage + slotOffset, sizeof(OffsetType));
	OffsetType newSlotSize = sizeof(OffsetType) + 2 * sizeof(MarkType) + fieldInfoSize + dataSize;

	if (oldSlotSize == newSlotSize)
	{
		OffsetType offset = slotOffset + sizeof(OffsetType) + sizeof(MarkType);
		MarkType version = fileHandle.getCurrentVersion();
		memcpy(finalPage + offset, &version, sizeof(MarkType));
		offset += sizeof(MarkType);
		memcpy(finalPage + offset, fieldInfo, fieldInfoSize);
		offset += fieldInfoSize;
		int nullFieldsIndicatorActualSize = ceil((double)recordDescriptor.size() / CHAR_BIT);
		memcpy(finalPage + offset, (char *)data + nullFieldsIndicatorActualSize, dataSize);
	}
	else if (oldSlotSize > newSlotSize)
	{
		OffsetType offset = slotOffset;
		memcpy(finalPage + offset, &newSlotSize, sizeof(OffsetType));
		offset += sizeof(OffsetType) + sizeof(MarkType);
		MarkType version = fileHandle.getCurrentVersion();
		memcpy(finalPage + offset, &version, sizeof(MarkType));
		offset += sizeof(MarkType);
		memcpy(finalPage + offset, fieldInfo, fieldInfoSize);
		offset += fieldInfoSize;
		int nullFieldsIndicatorActualSize = ceil((double)recordDescriptor.size() / CHAR_BIT);
		memcpy(finalPage + offset, (char *)data + nullFieldsIndicatorActualSize, dataSize);
		offset += dataSize;
		OffsetType slotCount;
		memcpy(&slotCount, finalPage + PAGE_SIZE - sizeof(OffsetType), sizeof(OffsetType));
		moveSlots(offset, slotNum + 1, slotCount, finalPage);
		//Decrease the size of page by target slot size
		OffsetType pageSize = fileHandle.allPagesSize[pageNum];
		pageSize += newSlotSize - oldSlotSize;
		memcpy(finalPage, &pageSize, sizeof(OffsetType));
		fileHandle.allPagesSize[pageNum] = pageSize;
	}
	else
	{
		OffsetType newPageSize = fileHandle.allPagesSize[pageNum] + newSlotSize - oldSlotSize;
		if (newPageSize <= PAGE_SIZE)
		{
			OffsetType slotEndOffset = slotOffset + newSlotSize;
			OffsetType slotCount;
			memcpy(&slotCount, finalPage + PAGE_SIZE - sizeof(OffsetType), sizeof(OffsetType));
			moveSlots(slotEndOffset, slotNum + 1, slotCount, finalPage);
			OffsetType offset = slotOffset;
			memcpy(finalPage + offset, &newSlotSize, sizeof(OffsetType));
			offset += sizeof(OffsetType) + sizeof(MarkType);
			MarkType version = fileHandle.getCurrentVersion();
			memcpy(finalPage + offset, &version, sizeof(MarkType));
			offset += sizeof(MarkType);
			memcpy(finalPage + offset, fieldInfo, fieldInfoSize);
			offset += fieldInfoSize;
			int nullFieldsIndicatorActualSize = ceil((double)recordDescriptor.size() / CHAR_BIT);
			memcpy(finalPage + offset, (char *)data + nullFieldsIndicatorActualSize, dataSize);
			//Decrease the size of page by target slot size
			OffsetType pageSize = fileHandle.allPagesSize[pageNum];
			pageSize += newSlotSize - oldSlotSize;
			memcpy(finalPage, &pageSize, sizeof(OffsetType));
			fileHandle.allPagesSize[pageNum] = pageSize;
		}
		else
		{
			RID newRid;
			status = insertRecord(fileHandle, recordDescriptor, data, newRid);
			if (status == -1)
			{
#ifdef DEBUG
				cerr << "Cannot insert a record into another page when updating a record exceeds the size of page" << endl;
#endif
				free(finalPage);
				return -1;
			}

			//When we are not updating the record in its original page and slot
			if (rid.pageNum != finalRid.pageNum || rid.slotNum != finalRid.slotNum)
			{
				PageNum startPageNum = rid.pageNum;
				OffsetType startSlotNum = rid.slotNum;
				char* startPage = (char*)malloc(PAGE_SIZE);
				status = fileHandle.readPage(startPageNum, startPage);
				if (status == -1)
				{
#ifdef DEBUG
					cerr << "Cannot read the start page " << startPageNum << " when updating record." << endl;
#endif
					free(startPage);
					free(finalPage);
					return -1;
				}
				OffsetType startSlotOffset;
				memcpy(&startSlotOffset, startPage + PAGE_SIZE - sizeof(OffsetType) * (startSlotNum + 2), sizeof(OffsetType));
				OffsetType offset = startSlotOffset + sizeof(OffsetType) + sizeof(MarkType);
				memcpy(startPage + offset, &(newRid.pageNum), sizeof(PageNum));
				offset += sizeof(PageNum);
				memcpy(startPage + offset, &(newRid.slotNum), sizeof(OffsetType));
				status = deleteRecord(fileHandle, recordDescriptor, finalRid);
				if (status == -1)
				{
#ifdef DEBUG
					cerr << "Cannot delete the final record when updating record." << endl;
#endif
					free(startPage);
					free(finalPage);
					return -1;
				}
				status = fileHandle.writePage(startPageNum, startPage);
				if (status == -1)
				{
#ifdef DEBUG
					cerr << "Cannot write the start page " << startPageNum << " when updating record." << endl;
#endif
					free(startPage);
					free(finalPage);
					return -1;
				}
				free(startPage);

				//Modify the isUpdatedRecord mark in the inserted record
				char* insertRecordPage = (char*)malloc(PAGE_SIZE);
				status = fileHandle.readPage(newRid.pageNum, insertRecordPage);
				if (status == -1)
				{
#ifdef DEBUG
					cerr << "Cannot read the insert page " << newRid.pageNum << " when updating record." << endl;
#endif
					free(insertRecordPage);
					free(finalPage);
					return -1;
				}
				OffsetType insertSlotOffset;
				memcpy(&insertSlotOffset, insertRecordPage + PAGE_SIZE - sizeof(OffsetType) * (newRid.slotNum + 2), sizeof(OffsetType));
				MarkType isUpdatedRecord = UpdatedRecordMark::UpdatedRecord;
				memcpy(insertRecordPage + insertSlotOffset + sizeof(OffsetType), &isUpdatedRecord, sizeof(MarkType));
				status = fileHandle.writePage(newRid.pageNum, insertRecordPage);
				if (status == -1)
				{
#ifdef DEBUG
					cerr << "Cannot write the insert page " << newRid.pageNum << " when updating record." << endl;
#endif
					free(insertRecordPage);
					free(finalPage);
					return -1;
				}
				free(insertRecordPage);
				free(finalPage);
				return 0;
			}
			else
			{
				OffsetType offset = slotOffset;
				OffsetType slotSize = sizeof(OffsetType) + sizeof(MarkType) + sizeof(PageNum) + sizeof(OffsetType);
				memcpy(finalPage + offset, &slotSize, sizeof(OffsetType));
				offset += sizeof(OffsetType);
				MarkType isUpdatedRecord = UpdatedRecordMark::Tombstone;
				memcpy(finalPage + offset, &isUpdatedRecord, sizeof(MarkType));
				offset += sizeof(MarkType);
				memcpy(finalPage + offset, &(newRid.pageNum), sizeof(PageNum));
				offset += sizeof(PageNum);
				memcpy(finalPage + offset, &(newRid.slotNum), sizeof(OffsetType));
				offset += sizeof(OffsetType);
				OffsetType slotCount;
				memcpy(&slotCount, finalPage + PAGE_SIZE - sizeof(OffsetType), sizeof(OffsetType));
				moveSlots(offset, slotNum + 1, slotCount, finalPage);
				//Decrease the size of page by target slot size
				OffsetType pageSize = fileHandle.allPagesSize[pageNum];
				pageSize += slotSize - oldSlotSize;
				memcpy(finalPage, &pageSize, sizeof(OffsetType));
				fileHandle.allPagesSize[pageNum] = pageSize;
			}
		}
	}
	status = fileHandle.writePage(pageNum, finalPage);
	if (status == -1)
	{
#ifdef DEBUG
		cerr << "Cannot write page back when updating record." << endl;
#endif
		free(finalPage);
		return -1;
	}
	free(finalPage);
	return 0;
}

RC RecordBasedFileManager::toFinalSlot(FileHandle &fileHandle, const RID &fromSlot, RID &finalSlot, char* &finalPage)
{
	PageNum currentPageNum = fromSlot.pageNum;
	OffsetType currentSlotNum = fromSlot.slotNum;
	char* currentPage = (char*)malloc(PAGE_SIZE);
	RC status = fileHandle.readPage(currentPageNum, currentPage);
	if (status == -1)
	{
#ifdef DEBUG
		cerr << "Cannot read page " << currentPageNum << " when traversing to final slot." << endl;
#endif
		finalPage = currentPage;
		return -1;
	}
	while (1)
	{
		OffsetType slotOffset;
		memcpy(&slotOffset, currentPage + PAGE_SIZE - sizeof(OffsetType) * (currentSlotNum + 2), sizeof(OffsetType));
		MarkType isUpdatedRecord;
		memcpy(&isUpdatedRecord, currentPage + slotOffset + sizeof(OffsetType), sizeof(MarkType));
		if (isUpdatedRecord == UpdatedRecordMark::Tombstone)
		{
			memcpy(&currentPageNum, currentPage + slotOffset + sizeof(OffsetType) + sizeof(MarkType), sizeof(PageNum));
			memcpy(&currentSlotNum, currentPage + slotOffset + sizeof(OffsetType) + sizeof(MarkType) + sizeof(PageNum), sizeof(OffsetType));
			status = fileHandle.readPage(currentPageNum, currentPage);
			if (status == -1)
			{
#ifdef DEBUG
				cerr << "Cannot read page " << currentPageNum << " when traversing to final slot." << endl;
#endif
				finalPage = currentPage;
				return -1;
			}
		}
		else
			break;
	}
	finalPage = currentPage;
	finalSlot.pageNum = currentPageNum;
	finalSlot.slotNum = currentSlotNum;
	return 0;
}

RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const string &attributeName, void *data)
{
	RID finalRid;
	char* finalPage;
	RC status = toFinalSlot(fileHandle, rid, finalRid, finalPage);
	if (status == -1)
	{
#ifdef DEBUG
		cerr << "Cannot traverse to final slot when reading attributes of record." << endl;
#endif
		free(finalPage);
		return -1;
	}
	unsigned int slotNum = finalRid.slotNum;

	for (size_t i = 0; i < recordDescriptor.size(); i++)
	{
		if (recordDescriptor[i].name == attributeName)
		{
			OffsetType slotOffset;
			memcpy(&slotOffset, finalPage + PAGE_SIZE - sizeof(OffsetType) * (slotNum + 2), sizeof(OffsetType));
			OffsetType attrOffset;
			memcpy(&attrOffset, finalPage + slotOffset + 2 * sizeof(MarkType) + sizeof(OffsetType) * (i + 2), sizeof(OffsetType));
			if (attrOffset != -1)
			{
				OffsetType attrLength;
				if (i == recordDescriptor.size() - 1)
				{
					OffsetType slotSize;
					memcpy(&slotSize, finalPage + slotOffset, sizeof(OffsetType));
					attrLength = slotOffset + slotSize - attrOffset;
				}
				else
				{
					OffsetType nextAttrOffset;
					memcpy(&nextAttrOffset, finalPage + slotOffset + 2 * sizeof(MarkType) + sizeof(OffsetType) * (i + 1 + 2), sizeof(OffsetType));
					attrLength = nextAttrOffset - attrOffset;
				}
				memcpy(data, finalPage + attrOffset, attrLength);
			}
			else
			{
#ifdef DEBUG
				cerr << "The attribute name " << attributeName << " contains null value" << endl;
#endif
			}
			free(finalPage);
			return 0;
		}
	}
#ifdef DEBUG
	cerr << "Cannot find the attribute name " << attributeName << endl;
#endif
	free(finalPage);
	return -1;
}

RC RecordBasedFileManager::scan(FileHandle &fileHandle,
	const vector<Attribute> &recordDescriptor,
	const string &conditionAttribute,
	const CompOp compOp,                  // comparision type such as "<" and "="
	const void *value,                    // used in the comparison
	const vector<string> &attributeNames, // a list of projected attributes
	RBFM_ScanIterator &rbfm_ScanIterator)
{
	rbfm_ScanIterator.setEnd(false);
	rbfm_ScanIterator.setCompOp(compOp);
	rbfm_ScanIterator.setValue(value);
	rbfm_ScanIterator.currentPageNum = 0;
	rbfm_ScanIterator.currentSlotNum = 0;
	rbfm_ScanIterator.setMaxPageNum(fileHandle.allPagesSize.size() - 1);
	rbfm_ScanIterator.setConditionField(-1);
	rbfm_ScanIterator.setFileHandle(fileHandle);
	rbfm_ScanIterator.setRecordDescriptor(recordDescriptor);

	vector<OffsetType>* outputFields = rbfm_ScanIterator.getOutputFields();
	bool findOutputField = false;
	for (size_t i = 0; i < recordDescriptor.size(); i++)
	{
		//Get the id of condition field
		if (recordDescriptor[i].name == conditionAttribute)
			rbfm_ScanIterator.setConditionField((OffsetType)i);
		for (size_t j = 0; j < attributeNames.size(); j++)
		{
			//Add the id of the field that we need to project
			if (recordDescriptor[i].name == attributeNames[j])
			{
				outputFields->push_back(i);
				findOutputField = true;
				break;
			}
		}
	}
	if (!findOutputField)
	{
#ifdef DEBUG
		cerr << "Cannot find the output fields when scanning" << endl;
#endif
		return -1;
	}
	return 0;
}

RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data)
{
	if (!end)
	{
		for (PageNum i = currentPageNum; i <= maxPageNum; i++)
		{
			char* pageData = (char*)malloc(PAGE_SIZE);
			RC status = fileHandle->readPage(i, pageData);
			if (status == -1)
			{
#ifdef DEBUG
				cerr << "Cannot read page " << i << " when getting the next record." << endl;
#endif
				free(pageData);
				return -1;
			}
			OffsetType slotCount;
			memcpy(&slotCount, pageData + PAGE_SIZE - sizeof(OffsetType), sizeof(OffsetType));
			OffsetType startSlot = 0;
			//If in the same page as last time
			if (i == currentPageNum)
				startSlot = currentSlotNum + 1;
			for (OffsetType j = startSlot; j < slotCount; j++)
			{
				OffsetType slotOffset;
				memcpy(&slotOffset, pageData + PAGE_SIZE - sizeof(OffsetType) * (j + 2), sizeof(OffsetType));
				//Whether the slot has been deleted
				if (slotOffset != -1)
				{
					MarkType isUpdatedRecord;
					memcpy(&isUpdatedRecord, pageData + slotOffset + sizeof(OffsetType), sizeof(MarkType));
					//Whether the slot is a tomb stone
					if (isUpdatedRecord != UpdatedRecordMark::UpdatedRecord)
					{
						if (isUpdatedRecord == UpdatedRecordMark::Tombstone)
						{
							RID fromRid;
							fromRid.pageNum = i;
							fromRid.slotNum = j;
							RID finalRid;
							free(pageData);
							status = RecordBasedFileManager::instance()->toFinalSlot(*fileHandle, fromRid, finalRid, pageData);
							if (status == -1)
							{
#ifdef DEBUG
								cerr << "Cannot read final page " << i << " when getting the next record." << endl;
#endif
								free(pageData);
								return -1;
							}
						}
						//Whether the query has a condition
						//If it has a condition and does not satisfy the condition, we will continue the loop and go to the next record
						if (conditionField != -1)
						{
							OffsetType conditionOffset;
							memcpy(&conditionOffset, pageData + slotOffset + 2 * sizeof(MarkType) + sizeof(OffsetType) * (2 + conditionField), sizeof(OffsetType));
							AttrType conditionType = recordDescriptor->at(conditionField).type;
							int compResult;
							if (conditionType == TypeInt)
							{
								compResult = memcmp(pageData + slotOffset + conditionOffset, value, sizeof(int));
							}
							else if (conditionType == TypeReal)
							{
								compResult = memcmp(pageData + slotOffset + conditionOffset, value, sizeof(float));
							}
							else if (conditionType == TypeVarChar)
							{
								int strLength;
								memcpy(&strLength, pageData + slotOffset + conditionOffset, sizeof(int));
								compResult = memcmp(pageData + slotOffset + conditionOffset + sizeof(int), value, strLength);
							}
							if (compResult < 0 && (compOp == GT_OP || compOp == GE_OP || compOp == EQ_OP))
								continue;
							if (compResult == 0 && (compOp == LT_OP || compOp == GT_OP || compOp == NE_OP))
								continue;
							if (compResult > 0 && (compOp == LT_OP || compOp == LE_OP || compOp == EQ_OP))
								continue;
						}
						//Get all the projected fields in the record and combine them
						int nullFieldsIndicatorActualSize = ceil((double)outputFields.size() / CHAR_BIT);
						unsigned char *nullFieldsIndicator = (unsigned char*)malloc(nullFieldsIndicatorActualSize);
						unsigned char nullFields = 0;
						OffsetType dataOffset = nullFieldsIndicatorActualSize;
						for (size_t k = 0; k < outputFields.size(); k++)
						{
							if (nullFields % 8 == 0)
								nullFields = 0;
							OffsetType fieldOffset;
							memcpy(&fieldOffset, pageData + slotOffset + 2 * sizeof(MarkType) + sizeof(OffsetType) * (2 + k), sizeof(OffsetType));
							if (fieldOffset == -1)
							{
								nullFields += 1 << (7 - k % 8);
							}
							else
							{
								AttrType attrType = recordDescriptor->at(outputFields[k]).type;
								int fieldLength = 0;
								if (attrType == TypeInt)
									fieldLength = sizeof(int);
								else if (attrType == TypeReal)
									fieldLength = sizeof(float);
								else if (attrType == TypeVarChar)
								{
									memcpy(&fieldLength, pageData + slotOffset + fieldOffset, sizeof(int));
									fieldLength += sizeof(int);
								}
								memcpy((char*)data + dataOffset, pageData + slotOffset + fieldOffset, fieldLength);
								dataOffset += fieldLength;
							}
							nullFieldsIndicator[k / 8] = nullFields;
						}
						memcpy((char*)data, nullFieldsIndicator, nullFieldsIndicatorActualSize);
						currentPageNum = i;
						currentSlotNum = j;
						rid.pageNum = currentPageNum;
						rid.slotNum = currentSlotNum;
						free(nullFieldsIndicator);
						free(pageData);
						return 0;
					}
				}
			}
			free(pageData);
		}
		end = true;
	}
	return RBFM_EOF;
}