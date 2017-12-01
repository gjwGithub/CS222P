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

RC RecordBasedFileManager::createFile(const string &fileName) 
{
	RC status = PagedFileManager::instance()->createFile(fileName);
	if (status)
	{
#ifdef DEBUG
		cerr << "Cannot create the file while creating file" << endl;
#endif
		return -1;
	}

	//Create metadata in page 0
	FILE *file = fopen(fileName.c_str(), "w");
	if (file == NULL)
	{
#ifdef DEBUG
		cerr << "File does not exist!" << endl;
#endif
		return -1;
	}
	unsigned readPageCounter = 0;
	unsigned writePageCounter = 0;
	unsigned appendPageCounter = 0;
	unsigned pageNum = 0;
	unsigned insertCount = 0;
	unsigned currentVersion = 0;
	char* metaData = (char*)calloc(PAGE_SIZE, 1);
	OffsetType offset = 0;
	memcpy(metaData + offset, &readPageCounter, sizeof(unsigned));
	offset += sizeof(unsigned);
	memcpy(metaData + offset, &writePageCounter, sizeof(unsigned));
	offset += sizeof(unsigned);
	memcpy(metaData + offset, &appendPageCounter, sizeof(unsigned));
	offset += sizeof(unsigned);
	memcpy(metaData + offset, &pageNum, sizeof(unsigned));
	offset += sizeof(unsigned);
	memcpy(metaData + offset, &insertCount, sizeof(unsigned));
	offset += sizeof(unsigned);
	memcpy(metaData + offset, &currentVersion, sizeof(unsigned));
	offset += sizeof(unsigned);
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

RC RecordBasedFileManager::destroyFile(const string &fileName) 
{
    return PagedFileManager::instance()->destroyFile(fileName);
}

RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle) 
{
	if (fileHandle.getFile())
	{
		int status = closeFile(fileHandle);
		if (status)
		{
#ifdef DEBUG
			cerr << "Cannot close the file before opening the file" << endl;
#endif
			return -1;
		}
	}
	RC status = PagedFileManager::instance()->openFile(fileName, fileHandle);
	if (status == -1)
	{
#ifdef DEBUG
		cerr << "Open file error in open file" << endl;
#endif
		return -1;
	}
	status = fileHandle.readMetaData();
	if (status == -1)
	{
#ifdef DEBUG
		cerr << "Read meta data error in open file" << endl;
#endif
		return -1;
	}
	status = fileHandle.generateAllPagesSize(fileHandle.allPagesSize);
	if (status == -1)
	{
#ifdef DEBUG
		cerr << "Generate all page sizes error in open file" << endl;
#endif
		return -1;
	}
	return 0;
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) 
{
	RC status = fileHandle.writeMetaData();
	if (status == -1)
	{
#ifdef DEBUG
		cerr << "Write meta data error in closing file" << endl;
#endif
		return -1;
	}
	status = PagedFileManager::instance()->closeFile(fileHandle);
	if (status == -1)
	{
#ifdef DEBUG
		cerr << "Write meta data error in closing file" << endl;
#endif
		return -1;
	}
	return 0;
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
			fieldOffset = NULLFIELD;
		if (fieldOffset != NULLFIELD)
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
		else
		{
			//If we reuse a previous deleted slot
			newPageSize -= sizeof(OffsetType); //Since we didn't change the size of slot table
			fileHandle.allPagesSize[pageNum] = newPageSize;
			OffsetType offset = 0;
			memcpy(pageData + offset, &newPageSize, sizeof(OffsetType));
			OffsetType targetSlotOffset;
			memcpy(&targetSlotOffset, pageData + PAGE_SIZE - sizeof(OffsetType) * (targetSlot + 2), sizeof(OffsetType));
			if (targetSlot < oldSlotCount - 1) // We need to move back other slots to make more space if we insert the slot in the middle
				moveSlots(targetSlotOffset + slotSize, targetSlot + 1, oldSlotCount - 1, pageData);
			offset = targetSlotOffset;
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
        ++fileHandle.insertCount;
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
	++fileHandle.insertCount;
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

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) 
{
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
	if (offset == DELETEDSLOT)
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
	memset(nullFieldsIndicator, 0, nullFieldsIndicatorActualSize);
	
	vector<Attribute>* versionTable = RelationManager::instance()->getVersionTable(version);
	vector<int> outputFields;
	for (size_t i = 0; i < recordDescriptor.size(); i++)
	{
		bool foundField = false;
		for (size_t j = 0; j < versionTable->size(); j++)
		{
			if (recordDescriptor[i].name == versionTable->at(j).name)
			{
				foundField = true;
				outputFields.push_back(j);
				break;
			}
		}
		if (!foundField)
			outputFields.push_back(NULLFIELD);
	}
	OffsetType dataOffset = nullFieldsIndicatorActualSize;
	for (size_t i = 0; i < outputFields.size(); i++)
	{
		unsigned char nullFields = nullFieldsIndicator[i / 8];
		if (outputFields[i] != NULLFIELD)
		{
			OffsetType fieldOffset;
			memcpy(&fieldOffset, pageData + offset + outputFields[i] * sizeof(OffsetType), sizeof(OffsetType));
			if (fieldOffset != NULLFIELD)
			{
				if (outputFields[i] < fieldNum - 1)
				{
					OffsetType nextFieldOffset;
					memcpy(&nextFieldOffset, pageData + offset + (outputFields[i] + 1) * sizeof(OffsetType), sizeof(OffsetType));
					OffsetType dataSize = nextFieldOffset - fieldOffset;
					memcpy((char*)data + dataOffset, pageData + startOffset + fieldOffset, dataSize);
					dataOffset += dataSize;
				}
				else
				{
					OffsetType dataSize = slotSize - fieldOffset;
					memcpy((char*)data + dataOffset, pageData + startOffset + fieldOffset, dataSize);
					dataOffset += dataSize;
				}
			}
			else
			{
				nullFields += 1 << (7 - i % 8);
			}
		}
		else
		{
			nullFields += 1 << (7 - i % 8);
		}
		nullFieldsIndicator[i / 8] = nullFields;
	}
	memcpy(data, nullFieldsIndicator, nullFieldsIndicatorActualSize);

	free(pageData);
	free(nullFieldsIndicator);
	return 0;
}

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) 
{
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
			if (slotOffset == DELETEDSLOT)
			{
				//If deleted slot is not the last slot in table
				if (i < slotCount - 1)
				{
					//Find the offset of an undeleted slot to replace the offset of current slot
					bool foundNotDeletedSlot = false;
					for (OffsetType j = i + 1; j < slotCount; j++)
					{
						OffsetType nextSlotOffset;
						memcpy(&nextSlotOffset, data + PAGE_SIZE - sizeof(OffsetType) * (j + 2), sizeof(OffsetType));
						if (nextSlotOffset != DELETEDSLOT)
						{
							memcpy(data + PAGE_SIZE - sizeof(OffsetType) * (i + 2), &nextSlotOffset, sizeof(OffsetType));
							foundNotDeletedSlot = true;
							break;
						}
					}
					//If all the slot after current slot were deleted, we will look forward to generate the offset of current slot
					if (!foundNotDeletedSlot)
					{
						//Whether current slot is the first slot in the slot table
						//If not, the slotOffset = previousSlotOffset + previousSlotSize
						if (i == 0)
						{
							OffsetType targetSlotOffset = sizeof(OffsetType);
							memcpy(data + PAGE_SIZE - sizeof(OffsetType) * (i + 2), &targetSlotOffset, sizeof(OffsetType));
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
					}
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
	//Delete both the tombstone and real record
	if (rid.pageNum != finalRid.pageNum || rid.slotNum != finalRid.slotNum)
	{
		status = deleteRecord(fileHandle, recordDescriptor, finalRid);
		if (status == -1)
		{
#ifdef DEBUG
			cerr << "Cannot delete real record when deleting record." << endl;
#endif
			free(finalPage);
			return -1;
		}
		status = fileHandle.readPage(rid.pageNum, finalPage);
		if (status == -1)
		{
#ifdef DEBUG
			cerr << "Cannot read page of tombstone when deleting record." << endl;
#endif
			free(finalPage);
			return -1;
		}
	}
	PageNum pageNum = rid.pageNum;
	unsigned int slotNum = rid.slotNum;
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
	//Find out whether the last slot in the slot table has been deleted before
	OffsetType lastSlotOffset;
	memcpy(&lastSlotOffset, finalPage + PAGE_SIZE - sizeof(OffsetType) * (slotCount - 1 + 2), sizeof(OffsetType));
	if (lastSlotOffset == DELETEDSLOT)
	{
		//We will remove all the deleted slot in the back
		int deletedSlotCount = 0;
		for (OffsetType i = slotCount - 1; i >= 0; i--)
		{
			OffsetType iSlotOffset;
			memcpy(&iSlotOffset, finalPage + PAGE_SIZE - sizeof(OffsetType) * (i + 2), sizeof(OffsetType));
			if (iSlotOffset == DELETEDSLOT)
				++deletedSlotCount;
			else
				break;
		}
		slotCount -= deletedSlotCount;
		memcpy(finalPage + PAGE_SIZE - sizeof(OffsetType), &slotCount, sizeof(OffsetType));
		pageSize -= deletedSlotCount * sizeof(OffsetType);
	}
	memcpy(finalPage, &pageSize, sizeof(OffsetType));
	fileHandle.allPagesSize[pageNum] = pageSize;
	//Move forward other slots if target slot is not the last one
	if ((OffsetType)slotNum < slotCount - 1)
		moveSlots(slotOffset, slotNum + 1, slotCount - 1, finalPage);
	//Change the target slot offset into -1
	slotOffset = DELETEDSLOT;
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
	//Find the first undeleted slot in this range ([startSlot, endSlot])
	bool foundStartSlot = false;
	OffsetType startOffset;
	for (OffsetType i = startSlot; i <= endSlot; i++)
	{
		memcpy(&startOffset, pageData + PAGE_SIZE - sizeof(OffsetType) * (i + 2), sizeof(OffsetType));
		if (startOffset != DELETEDSLOT)
		{
			foundStartSlot = true;
			break;
		}
	}
	//If all the slots in this range has been deleted before
	if (!foundStartSlot)
		return;
	OffsetType endOffset;
	for (OffsetType i = endSlot; i >= startSlot; i--)
	{
		memcpy(&endOffset, pageData + PAGE_SIZE - sizeof(OffsetType) * (i + 2), sizeof(OffsetType));
		if (endOffset != DELETEDSLOT)
			break;
	}
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
		//If the slot has been deleted before, then we do not need to change its offset, which is -1
		if (currentSlotOffset != DELETEDSLOT)
		{
			currentSlotOffset += deltaOffset;
			memcpy(pageData + PAGE_SIZE - sizeof(OffsetType) * (i + 2), &currentSlotOffset, sizeof(OffsetType));
		}
	}
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
		free(fieldInfo);
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
		moveSlots(offset, slotNum + 1, slotCount - 1, finalPage);
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
			moveSlots(slotEndOffset, slotNum + 1, slotCount - 1, finalPage);
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
			//Increase the size of page by target slot size
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
				free(fieldInfo);
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
					free(fieldInfo);
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
					free(fieldInfo);
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
					free(fieldInfo);
					free(startPage);
					free(finalPage);
					return -1;
				}
				free(startPage);
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
				moveSlots(offset, slotNum + 1, slotCount - 1, finalPage);
				//Decrease the size of page by target slot size
				OffsetType pageSize = fileHandle.allPagesSize[pageNum];
				pageSize += slotSize - oldSlotSize;
				memcpy(finalPage, &pageSize, sizeof(OffsetType));
				fileHandle.allPagesSize[pageNum] = pageSize;
			}

			//Modify the isUpdatedRecord mark in the inserted record
			char* insertRecordPage = (char*)malloc(PAGE_SIZE);
			status = fileHandle.readPage(newRid.pageNum, insertRecordPage);
			if (status == -1)
			{
#ifdef DEBUG
				cerr << "Cannot read the insert page " << newRid.pageNum << " when updating record." << endl;
#endif
				free(fieldInfo);
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
				free(fieldInfo);
				free(insertRecordPage);
				free(finalPage);
				return -1;
			}
			free(insertRecordPage);

			//If we are not updating the record in its original page and slot, we do not need to write back the final page since we deleted an record in that page before.
			if (rid.pageNum != finalRid.pageNum || rid.slotNum != finalRid.slotNum)
			{
				free(fieldInfo);
				free(finalPage);
				return 0;
			}
		}
	}
	status = fileHandle.writePage(pageNum, finalPage);
	if (status == -1)
	{
#ifdef DEBUG
		cerr << "Cannot write page back when updating record." << endl;
#endif
		free(fieldInfo);
		free(finalPage);
		return -1;
	}
	free(fieldInfo);
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
		//Whether this slot has been deleted before
		if (slotOffset == DELETEDSLOT)
		{
#ifdef DEBUG
			cerr << "Cannot read a deleted slot " << currentSlotNum << " when traversing to final slot." << endl;
#endif
			finalPage = currentPage;
			return -1;
		}
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

	OffsetType slotOffset;
	memcpy(&slotOffset, finalPage + PAGE_SIZE - sizeof(OffsetType) * (slotNum + 2), sizeof(OffsetType));
	MarkType version;
	memcpy(&version, finalPage + slotOffset + sizeof(OffsetType) + sizeof(MarkType), sizeof(MarkType));
	vector<Attribute> *versionTable = RelationManager::instance()->getVersionTable(version);
	for (size_t i = 0; i < versionTable->size(); i++)
	{
		if (versionTable->at(i).name == attributeName)
		{
			OffsetType attrOffset;
			memcpy(&attrOffset, finalPage + slotOffset + 2 * sizeof(MarkType) + sizeof(OffsetType) * (i + 2), sizeof(OffsetType));
			if (attrOffset != NULLFIELD)
			{
				OffsetType attrLength;
				if (i == versionTable->size() - 1)
				{
					OffsetType slotSize;
					memcpy(&slotSize, finalPage + slotOffset, sizeof(OffsetType));
					attrLength = slotSize - attrOffset;
				}
				else
				{
					OffsetType nextAttrOffset;
					memcpy(&nextAttrOffset, finalPage + slotOffset + 2 * sizeof(MarkType) + sizeof(OffsetType) * (i + 1 + 2), sizeof(OffsetType));
					attrLength = nextAttrOffset - attrOffset;
				}
				unsigned char nullFields = 0;
				int nullFieldsIndicatorActualSize = ceil((double)1 / CHAR_BIT);
				memcpy(data, &nullFields, nullFieldsIndicatorActualSize);
				memcpy((char*)data + nullFieldsIndicatorActualSize, finalPage + slotOffset + attrOffset, attrLength);
			}
			else
			{
#ifdef DEBUG
				cout << "The attribute name " << attributeName << " contains null value" << endl;
#endif
				unsigned char nullFields = 1 << 7;
				int nullFieldsIndicatorActualSize = ceil((double)1 / CHAR_BIT);
				memcpy(data, &nullFields, nullFieldsIndicatorActualSize);
			}
			free(finalPage);
			return 0;
		}
	}
#ifdef DEBUG
	cout << "Cannot find the attribute name " << attributeName << endl;
#endif
	unsigned char nullFields = 1 << 7;
	int nullFieldsIndicatorActualSize = ceil((double)1 / CHAR_BIT);
	memcpy(data, &nullFields, nullFieldsIndicatorActualSize);
	free(finalPage);
	return 0;
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
	rbfm_ScanIterator.currentPageNum = 0;
	rbfm_ScanIterator.currentSlotNum = -1; //We will start at rid = (0,0) next time
	rbfm_ScanIterator.setMaxPageNum(fileHandle.allPagesSize.size() - 1);
	rbfm_ScanIterator.setConditionField(NULLFIELD);
	rbfm_ScanIterator.setFileHandle(fileHandle);
	rbfm_ScanIterator.setRecordDescriptor(recordDescriptor);

	for (size_t i = 0; i < recordDescriptor.size(); i++)
	{
		//Get the id of condition field
		if (recordDescriptor[i].name == conditionAttribute)
		{
			rbfm_ScanIterator.setConditionField((OffsetType)i);
			break;
		}
	}

	rbfm_ScanIterator.setAttributeNames(attributeNames);
	rbfm_ScanIterator.setValue(value);
	return 0;
}

RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data)
{
	if (!end)
	{
		for (int i = currentPageNum; i <= maxPageNum; i++)
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
			if (i == (int)currentPageNum)
				startSlot = currentSlotNum + 1;
			for (OffsetType j = startSlot; j < slotCount; j++)
			{
				OffsetType slotOffset;
				memcpy(&slotOffset, pageData + PAGE_SIZE - sizeof(OffsetType) * (j + 2), sizeof(OffsetType));
				//Whether the slot has been deleted
				if (slotOffset != DELETEDSLOT)
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
							memcpy(&slotOffset, pageData + PAGE_SIZE - sizeof(OffsetType) * (finalRid.slotNum + 2), sizeof(OffsetType));
						}

						MarkType version;
						memcpy(&version, pageData + slotOffset + sizeof(OffsetType) + sizeof(MarkType), sizeof(MarkType));
						
						//Get the position of the previous condition field
						bool findConditionField = false;
						OffsetType oldConditionField = NULLFIELD;
						for (size_t k = 0; k < versionTable[version].size(); k++)
						{
							//Get the id of condition field
							if (this->conditionField != NULLFIELD && versionTable[version][k].name == recordDescriptor.at(this->conditionField).name)
							{
								oldConditionField = (OffsetType)k;
								findConditionField = true;
								break;
							}
						}
						//If the condition field only appears in new schema
						if (this->conditionField != NULLFIELD && !findConditionField)
							oldConditionField = NULLFIELD;
						//Get the positions of the output fields in the previous record
						vector<OffsetType> outputFieldsOldPositions;
						for (size_t k = 0; k < this->attributeNames.size(); k++)
						{
							bool findOutputField = false;
							for (size_t l = 0; l < versionTable[version].size(); l++)
							{
								//Add the id of the field that we need to project
								if (versionTable[version][l].name == this->attributeNames[k])
								{
									outputFieldsOldPositions.push_back(l);
									findOutputField = true;
									break;
								}
							}
							if (!findOutputField)
								outputFieldsOldPositions.push_back(NULLFIELD);
						}
						//Get the positions of the output fields in the current record
						vector<OffsetType> outputFieldsCurrentPositions;
						for (size_t k = 0; k < this->attributeNames.size(); k++)
						{
							for (size_t l = 0; l < recordDescriptor.size(); l++)
							{
								//Add the id of the field that we need to project
								if (recordDescriptor[l].name == this->attributeNames[k])
								{
									outputFieldsCurrentPositions.push_back(l);
									break;
								}
							}
						}

						//Whether the query has a condition
						//If it has a condition and does not satisfy the condition, we will continue the loop and go to the next record
						if (oldConditionField != NULLFIELD)
						{
							OffsetType conditionOffset;
							memcpy(&conditionOffset, pageData + slotOffset + 2 * sizeof(MarkType) + sizeof(OffsetType) * (2 + oldConditionField), sizeof(OffsetType));
							AttrType conditionType = versionTable[version].at(oldConditionField).type;
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
								int conditionStrLength;
								memcpy(&conditionStrLength, value, sizeof(int));
								compResult = strLength - conditionStrLength;
								if (compResult == 0)
									compResult = memcmp(pageData + slotOffset + conditionOffset + sizeof(int), (char*)value + sizeof(int), strLength);
							}
							if (compResult < 0 && (compOp == GT_OP || compOp == GE_OP || compOp == EQ_OP))
								continue;
							if (compResult == 0 && (compOp == LT_OP || compOp == GT_OP || compOp == NE_OP))
								continue;
							if (compResult > 0 && (compOp == LT_OP || compOp == LE_OP || compOp == EQ_OP))
								continue;
						}

						//Get all the projected fields in the record and combine them
						int nullFieldsIndicatorActualSize = ceil((double)outputFieldsOldPositions.size() / CHAR_BIT);
						unsigned char *nullFieldsIndicator = (unsigned char*)malloc(nullFieldsIndicatorActualSize);
						unsigned char nullFields = 0;
						OffsetType dataOffset = nullFieldsIndicatorActualSize;
						for (size_t k = 0; k < outputFieldsOldPositions.size(); k++)
						{
							if (k % 8 == 0)
								nullFields = 0;
							if (outputFieldsOldPositions[k] == NULLFIELD)
							{
								nullFields += 1 << (7 - k % 8);
								nullFieldsIndicator[k / 8] = nullFields;
								continue;
							}
							OffsetType fieldOffset;
							memcpy(&fieldOffset, pageData + slotOffset + 2 * sizeof(MarkType) + sizeof(OffsetType) * (2 + outputFieldsOldPositions[k]), sizeof(OffsetType));
							if (fieldOffset == NULLFIELD)
							{
								nullFields += 1 << (7 - k % 8);
							}
							else
							{
								AttrType attrType = recordDescriptor.at(outputFieldsCurrentPositions[k]).type;
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
						//cout << "nullIndicator: " << (int)*nullFieldsIndicator << endl;
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