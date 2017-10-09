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
	unsigned char *nullFieldsIndicator = new unsigned char[nullFieldsIndicatorActualSize];
	memcpy(nullFieldsIndicator, (char *)data + dataOffset, nullFieldsIndicatorActualSize);
	dataOffset += nullFieldsIndicatorActualSize;

	OffsetType fieldsNum = recordDescriptor.size();
	resultLength = sizeof(OffsetType) + fieldsNum * sizeof(OffsetType);
	result = new char[resultLength];
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
	delete nullFieldsIndicator;
	dataSize = dataOffset - nullFieldsIndicatorActualSize;
}

RC RecordBasedFileManager::appendDataInPage(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, int pageNum, const void *data, RID &rid, OffsetType &fieldInfoSize, char* &fieldInfo, OffsetType &dataSize)
{
	OffsetType oldPageSize = this->allPagesSize[pageNum];
	OffsetType slotSize = sizeof(OffsetType) + fieldInfoSize + dataSize; // slotSizeNumber + fieldInfoSize + dataSize
	OffsetType newPageSize = oldPageSize + slotSize + sizeof(OffsetType);
    if (newPageSize <= PAGE_SIZE)
    {
		char *pageData = new char[PAGE_SIZE];
		RC status = fileHandle.readPage(pageNum, pageData);
		if (status == -1)
		{
#ifdef DEBUG
			cerr << "Cannot read page " << pageNum << " while append data in that page" << endl;
#endif
			delete pageData;
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
			delete pageData;
            return -1;
        }
        rid.pageNum = pageNum;
        rid.slotNum = slotCount;
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
    char *newData = new char[PAGE_SIZE];
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
		delete newData;
        return -1;
    }
	int totalPageNum = fileHandle.getNumberOfPages();
	rid.pageNum = totalPageNum - 1;
	rid.slotNum = 0;
    delete newData;
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
			delete fieldInfo;
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
			delete fieldInfo;
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
					delete fieldInfo;
					return -1;
				}
				else if (status == 1)
				{
					delete fieldInfo;
					return 0;
				}
            }
			status = addDataInNewPage(fileHandle, recordDescriptor, data, rid, fieldInfoSize, fieldInfo, dataSize);
			if (status == -1)
			{
#ifdef DEBUG
				cerr << "Cannot append a page while insert a record and all the other pages are full" << endl;
#endif
				delete fieldInfo;
				return -1;
			}
			delete fieldInfo;
			return 0;
        }  
    }
	delete fieldInfo;
    return 0;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
	PageNum pageNum = rid.pageNum;
	unsigned int slotNum = rid.slotNum;
	char* pageData = new char[PAGE_SIZE];
	RC status = fileHandle.readPage(pageNum, pageData);
	if (status == -1)
	{
#ifdef DEBUG
		cerr << "Cannot read page " << pageNum << endl;
#endif
		delete pageData;
		return -1;
	}
	
	OffsetType slotCount;
	memcpy(&slotCount, pageData + PAGE_SIZE - sizeof(OffsetType), sizeof(OffsetType));
	if ((OffsetType)slotNum >= slotCount)
	{
#ifdef DEBUG
		cerr << "SlotNum " << slotNum << " in RID is larger than the number of slots in this page!" << endl;
#endif
		delete pageData;
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
	unsigned char *nullFieldsIndicator = new unsigned char[nullFieldsIndicatorActualSize];
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
	delete pageData;
	delete nullFieldsIndicator;
	return 0;
}

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) {
	OffsetType offset = 0;
	bool nullBit = false;
	int nullFieldsIndicatorActualSize = ceil((double)recordDescriptor.size() / CHAR_BIT);
	unsigned char *nullFieldsIndicator = new unsigned char[nullFieldsIndicatorActualSize];
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
				char* str = new char[strLength + 1];
				str[strLength] = '\0';
				memcpy(str, (char *)data + offset, strLength);
				offset += strLength;
				cout << str << "\t";
				delete str;
			}
		}
		else
			cout << "Null\t";
	}
	cout << endl;
	delete nullFieldsIndicator;
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
