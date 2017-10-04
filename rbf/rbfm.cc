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

//int RecordBasedFileManager::getDataSize(const vector<Attribute> &recordDescriptor, const void *data)
//{
//    int offset = 0;
//    bool nullBit = false;
//    int nullFieldsIndicatorActualSize = ceil((double) recordDescriptor.size() / CHAR_BIT);
//    unsigned char *nullFieldsIndicator = new unsigned char[nullFieldsIndicatorActualSize];
//    memcpy(nullFieldsIndicator, (char *)data + offset, nullFieldsIndicatorActualSize);
//    offset += nullFieldsIndicatorActualSize;
//    for (size_t i = 0; i < recordDescriptor.size(); i++)
//    {
//        nullBit = nullFieldsIndicator[i / CHAR_BIT] & (1 << (CHAR_BIT - 1 - i % CHAR_BIT));
//        if (!nullBit) 
//        {
//            AttrType type = recordDescriptor[i].type;
//            if (type == TypeInt)
//            {
//                offset += sizeof(int);
//            }
//            else if (type == TypeReal)
//            {
//                offset += sizeof(float);
//            }
//            else if (type == TypeVarChar)
//            {
//                int strLength;
//                memcpy(&strLength, (char *)data + offset, sizeof(int));
//                offset += sizeof(int);
//                offset += strLength;
//            }    
//        }
//    }
//    delete nullFieldsIndicator;
//    return offset - nullFieldsIndicatorActualSize;
//}

void RecordBasedFileManager::generateFieldInfo(const vector<Attribute> &recordDescriptor, const void *data, int &resultLength, char* &result, int &dataSize)
{
	int dataOffset = 0;
	bool nullBit = false;
	int nullFieldsIndicatorActualSize = ceil((double)recordDescriptor.size() / CHAR_BIT);
	unsigned char *nullFieldsIndicator = new unsigned char[nullFieldsIndicatorActualSize];
	memcpy(nullFieldsIndicator, (char *)data + dataOffset, nullFieldsIndicatorActualSize);
	dataOffset += nullFieldsIndicatorActualSize;
	vector<int> fieldOffsets;
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
	int fieldsNum = fieldOffsets.size();
	resultLength = sizeof(int) + fieldsNum * sizeof(int);
	result = new char[resultLength];
	int offset = 0;
	memcpy(result + offset, &fieldsNum, sizeof(int));
	offset += sizeof(int);
	for (size_t i = 0; i < fieldOffsets.size(); i++)
	{
		if (fieldOffsets[i] != -1)
			fieldOffsets[i] += sizeof(int) + resultLength - nullFieldsIndicatorActualSize;
		memcpy(result + offset, &fieldOffsets[i], sizeof(int));
		offset += sizeof(int);
	}
	dataSize = dataOffset - nullFieldsIndicatorActualSize;
}

RC RecordBasedFileManager::appendDataInPage(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, int pageNum, const void *data, RID &rid)
{
    int oldPageOffset;
    char *pageData = new char[PAGE_SIZE];
    RC status = fileHandle.readPage(pageNum, pageData);
    if (status == -1)
    {
        cerr << "Cannot read page " << pageNum << endl;
		delete pageData;
        return -1;
    }
    memcpy(&oldPageOffset, pageData, sizeof(int));
	int dataSize;
	int fieldInfoSize;
	char* fieldInfo;
	generateFieldInfo(recordDescriptor, data, fieldInfoSize, fieldInfo, dataSize);
	int slotSize = sizeof(int) + fieldInfoSize + dataSize;
	int nullFieldsIndicatorActualSize = ceil((double)recordDescriptor.size() / CHAR_BIT);
    if (oldPageOffset + slotSize <= PAGE_SIZE)
    {
        int newPageOffset = oldPageOffset + slotSize;
        int offset = 0;
        memcpy(pageData + offset, &newPageOffset, sizeof(int));
        offset += oldPageOffset;
        memcpy(pageData + offset, &slotSize, sizeof(int));
        offset += sizeof(int);
		memcpy(pageData + offset, fieldInfo, fieldInfoSize);
		offset += fieldInfoSize;
        memcpy(pageData + offset, (char *)data + nullFieldsIndicatorActualSize, dataSize);
        status = fileHandle.writePage(pageNum, pageData);
        if (status == -1)
        {
            cerr << "Cannot write page " << pageNum << endl;
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
	int dataSize;
	int fieldInfoSize;
	char* fieldInfo;
	generateFieldInfo(recordDescriptor, data, fieldInfoSize, fieldInfo, dataSize);
	int slotSize = sizeof(int) + fieldInfoSize + dataSize;
	int nullFieldsIndicatorActualSize = ceil((double)recordDescriptor.size() / CHAR_BIT);
    char *newData = new char[sizeof(int) + slotSize];
    int offset = 0;
    int pageSize = sizeof(int) + slotSize;
    //int slotSize = sizeof(int) + dataSize;
    memcpy(newData + offset, &pageSize, sizeof(int));
    offset += sizeof(int);
    memcpy(newData + offset, &slotSize, sizeof(int));
    offset += sizeof(int);
	memcpy(newData + offset, fieldInfo, fieldInfoSize);
	offset += fieldInfoSize;
	//cout << "fieldInfoSize: " << fieldInfoSize << endl;
	memcpy(newData + offset, (char *)data + nullFieldsIndicatorActualSize, dataSize);
	//cout << "datasize: " << dataSize << endl;
	/*int field4;
	memcpy(&field4, newData + sizeof(int) *(3 + 3), sizeof(int));
	int vfield4;
	memcpy(&vfield4, newData + sizeof(int) + field4, sizeof(int));
	cout << "field4: " << vfield4 << endl;*/
    RC status = fileHandle.appendPage(newData);
    if (status == -1)
    {
        cerr << "Cannot append a page while insert a record" << endl;
		delete newData;
		delete fieldInfo;
        return -1;
    }
    int totalPageNum = fileHandle.getNumberOfPages();
    rid.pageNum = totalPageNum - 1;
    rid.slotNum = sizeof(int);
    delete newData;
	delete fieldInfo;
    return 0;
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid) {
    int totalPageNum = fileHandle.getNumberOfPages();
    //int dataSize = getDataSize(recordDescriptor, data);
    //int slotSize = sizeof(int) + dataSize;
    if (totalPageNum == 0)
    {
        RC status = addDataInNewPage(fileHandle, recordDescriptor, data, rid);
        if (status == -1)
            return -1;
    }
    else
    {
        int status = appendDataInPage(fileHandle, recordDescriptor, totalPageNum - 1, data, rid);
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
	int pageNum = rid.pageNum;
	int slotNum = rid.slotNum;
	char* pageData = new char[PAGE_SIZE];
	RC status = fileHandle.readPage(pageNum, pageData);
	if (status == -1)
	{
		cerr << "Cannot read page " << pageNum << endl;
		delete pageData;
		return -1;
	}
	int slotSize;
	int offset = slotNum;
	memcpy(&slotSize, pageData + offset, sizeof(int));
	offset += sizeof(int);
	int fieldNum;
	memcpy(&fieldNum, pageData + offset, sizeof(int));
	offset += sizeof(int);
	int nullFieldsIndicatorActualSize = ceil((double)recordDescriptor.size() / CHAR_BIT);
	unsigned char *nullFieldsIndicator = new unsigned char[nullFieldsIndicatorActualSize];
	for (int i = 0; i < fieldNum; i += 8)
	{
		unsigned char nullFields = 0;
		for (int j = 0; i + j < fieldNum && j < 8; j++)
		{
			int fieldOffset;
			memcpy(&fieldOffset, pageData + offset, sizeof(int));
			offset += sizeof(int);
			if (fieldOffset == -1)
				nullFields += 1 << (7 - j);
		}
		nullFieldsIndicator[i / 8] = nullFields;
	}
	memcpy(data, nullFieldsIndicator, nullFieldsIndicatorActualSize);
	int dataSize = slotSize - offset + slotNum;
	//cout << "datasize" << dataSize << " slotsize" << slotSize << endl;
	memcpy((char*)data + nullFieldsIndicatorActualSize, pageData + offset, dataSize);
	delete pageData;
	delete nullFieldsIndicator;
	return 0;
}

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) {
	int offset = 0;
	bool nullBit = false;
	int nullFieldsIndicatorActualSize = ceil((double)recordDescriptor.size() / CHAR_BIT);
	unsigned char *nullFieldsIndicator = new unsigned char[nullFieldsIndicatorActualSize];
	memcpy(nullFieldsIndicator, (char *)data + offset, nullFieldsIndicatorActualSize);
	offset += nullFieldsIndicatorActualSize;
	for (size_t i = 0; i < recordDescriptor.size(); i++)
	{
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
