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
    return PagedFileManager::instance()->openFile(fileName, fileHandle);
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
	vector<OffsetType> fieldOffsets;
	for (size_t i = 0; i < recordDescriptor.size(); i++)
	{
		nullBit = nullFieldsIndicator[i / CHAR_BIT] & (1 << (CHAR_BIT - 1 - i % CHAR_BIT));
		if (!nullBit)
		{
			AttrType type = recordDescriptor[i].type;
			if (type == TypeInt)
			{
				fieldOffsets.push_back(dataOffset);
				dataOffset += sizeof(int);
			}
			else if (type == TypeReal)
			{
				fieldOffsets.push_back(dataOffset);
				dataOffset += sizeof(float);
			}
			else if (type == TypeVarChar)
			{
				fieldOffsets.push_back(dataOffset);
				int strLength;
				memcpy(&strLength, (char *)data + dataOffset, sizeof(int));
				dataOffset += sizeof(int);
				dataOffset += strLength;
			}
		}
		else
			fieldOffsets.push_back(-1);
	}
	delete nullFieldsIndicator;
	size_t fieldsNum = fieldOffsets.size();
	resultLength = sizeof(OffsetType) + fieldsNum * sizeof(OffsetType);
	result = new char[resultLength];
	OffsetType offset = 0;
	memcpy(result + offset, &fieldsNum, sizeof(int));
	offset += sizeof(OffsetType);
	for (size_t i = 0; i < fieldOffsets.size(); i++)
	{
		if (fieldOffsets[i] != -1)
			fieldOffsets[i] += sizeof(OffsetType) + resultLength - nullFieldsIndicatorActualSize;
		memcpy(result + offset, &fieldOffsets[i], sizeof(OffsetType));
		offset += sizeof(OffsetType);
	}
	dataSize = dataOffset - nullFieldsIndicatorActualSize;
}

RC RecordBasedFileManager::appendDataInPage(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, int pageNum, const void *data, RID &rid)
{
	OffsetType oldPageOffset;
    char *pageData = new char[PAGE_SIZE];
    RC status = fileHandle.readPage(pageNum, pageData);
    if (status == -1)
    {
#ifdef DEBUG
		cerr << "Cannot read page " << pageNum << endl;
#endif
		delete pageData;
        return -1;
    }
    memcpy(&oldPageOffset, pageData, sizeof(OffsetType));
	OffsetType dataSize;
	OffsetType fieldInfoSize;
	char* fieldInfo;
	generateFieldInfo(recordDescriptor, data, fieldInfoSize, fieldInfo, dataSize);
	OffsetType slotSize = sizeof(OffsetType) + fieldInfoSize + dataSize;
	int nullFieldsIndicatorActualSize = ceil((double)recordDescriptor.size() / CHAR_BIT);
    if (oldPageOffset + slotSize <= PAGE_SIZE)
    {
		OffsetType newPageOffset = oldPageOffset + slotSize;
		OffsetType offset = 0;
        memcpy(pageData + offset, &newPageOffset, sizeof(OffsetType));
        offset += oldPageOffset;
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
			delete pageData;
			delete fieldInfo;
            return -1;
        }
        rid.pageNum = pageNum;
        rid.slotNum = oldPageOffset;
    }
    else
    {
        delete pageData;
		delete fieldInfo;
        return 0;
    }
    delete pageData;
	delete fieldInfo;
    return 1;
}

RC RecordBasedFileManager::addDataInNewPage(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid)
{
	OffsetType dataSize;
	OffsetType fieldInfoSize;
	char* fieldInfo;
	generateFieldInfo(recordDescriptor, data, fieldInfoSize, fieldInfo, dataSize);
	OffsetType slotSize = sizeof(OffsetType) + fieldInfoSize + dataSize;
	int nullFieldsIndicatorActualSize = ceil((double)recordDescriptor.size() / CHAR_BIT);
	OffsetType pageSize = sizeof(OffsetType) + slotSize;
    char *newData = new char[pageSize];
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
		delete newData;
		delete fieldInfo;
        return -1;
    }
    int totalPageNum = fileHandle.getNumberOfPages();
    rid.pageNum = totalPageNum - 1;
    rid.slotNum = sizeof(OffsetType);
    delete newData;
	delete fieldInfo;
    return 0;
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid) {
    int totalPageNum = fileHandle.getNumberOfPages();
    if (totalPageNum == 0)
    {
        RC status = addDataInNewPage(fileHandle, recordDescriptor, data, rid);
        if (status == -1)
            return -1;
    }
    else
    {
        RC status = appendDataInPage(fileHandle, recordDescriptor, totalPageNum - 1, data, rid);
        if (status == -1)
            return -1;
        else if (status == 0)
        {
            for (int i = 0; i < totalPageNum - 1; i++)
            {
                status = appendDataInPage(fileHandle, recordDescriptor, i, data, rid);
                if (status == -1)
                    return -1;
                else if (status == 1)
                    return 0;
            }
        }
        else if (status == 1)
        {
            status = addDataInNewPage(fileHandle, recordDescriptor, data, rid);
            if (status == -1)
                return -1;
            return 0;
        }    
    }
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
	OffsetType slotSize;
	OffsetType offset = slotNum;
	memcpy(&slotSize, pageData + offset, sizeof(OffsetType));
	offset += sizeof(OffsetType);
	int fieldNum;
	memcpy(&fieldNum, pageData + offset, sizeof(OffsetType));
	offset += sizeof(OffsetType);
	int nullFieldsIndicatorActualSize = ceil((double)recordDescriptor.size() / CHAR_BIT);
	unsigned char *nullFieldsIndicator = new unsigned char[nullFieldsIndicatorActualSize];
	for (int i = 0; i < fieldNum; i += 8)
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
	OffsetType dataSize = slotSize - offset + slotNum;
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
