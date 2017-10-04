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

int RecordBasedFileManager::getDataSize(const vector<Attribute> &recordDescriptor, const void *data)
{
    int offset = 0;
    bool nullBit = false;
    int nullFieldsIndicatorActualSize = ceil((double) recordDescriptor.size() / CHAR_BIT);
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
                offset += sizeof(int);
            }
            else if (type == TypeReal)
            {
                offset += sizeof(float);
            }
            else if (type == TypeVarChar)
            {
                int strLength;
                memcpy(&strLength, (char *)data + offset, sizeof(int));
                offset += sizeof(int);
                offset += strLength;
            }    
        }
    }
    delete nullFieldsIndicator;
    return offset;
}

RC RecordBasedFileManager::appendDataInPage(FileHandle &fileHandle, int pageNum, const void *data, int slotSize, RID &rid)
{
    int oldPageOffset;
    char *pageData = new char[PAGE_SIZE];
    RC status = fileHandle.readPage(pageNum, pageData);
    if (status == -1)
    {
        cerr << "Cannot read page " << pageNum << endl;
        return -1;
    }
    memcpy(&oldPageOffset, pageData, sizeof(int));
    if (oldPageOffset + slotSize <= PAGE_SIZE)
    {
        int newPageOffset = oldPageOffset + slotSize;
        int offset = 0;
        memcpy(pageData + offset, &newPageOffset, sizeof(int));
        offset += oldPageOffset;
        memcpy(pageData + offset, &slotSize, sizeof(int));
        offset += sizeof(int);
        memcpy(pageData + offset, data, sizeof(int));
        status = fileHandle.writePage(pageNum, pageData);
        if (status == -1)
        {
            cerr << "Cannot write page " << pageNum << endl;
            return -1;
        }
        rid.pageNum = pageNum;
        rid.slotNum = oldPageOffset;
    }
    else
    {
        delete pageData;
        return 0;
    }
    delete pageData;
    return 1;
}

RC RecordBasedFileManager::addDataInNewPage(FileHandle &fileHandle, const void *data, int dataSize, RID &rid)
{
    char *newData = new char[sizeof(int) * 2 + dataSize];
    int offset = 0;
    int pageSize = sizeof(int) * 2 + dataSize;
    int slotSize = sizeof(int) + dataSize;
    memcpy(newData + offset, &pageSize, sizeof(int));
    offset += sizeof(int);
    memcpy(newData + offset, &slotSize, sizeof(int));
    offset += sizeof(int);
    memcpy(newData + offset, data, dataSize);
    RC status = fileHandle.appendPage(newData);
    if (status == -1)
    {
        cerr << "Cannot append a page while insert a record" << endl;
        return -1;
    }
    int totalPageNum = fileHandle.getNumberOfPages();
    rid.pageNum = totalPageNum - 1;
    rid.slotNum = sizeof(int);
    delete newData;
    return 0;
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid) {
    int totalPageNum = fileHandle.getNumberOfPages();
    int dataSize = getDataSize(recordDescriptor, data);
    int slotSize = sizeof(int) + dataSize;
    if (totalPageNum == 0)
    {
        RC status = addDataInNewPage(fileHandle, data, dataSize, rid);
        if (status == -1)
            return -1;
    }
    else
    {
        int status = appendDataInPage(fileHandle, totalPageNum - 1, data, slotSize, rid);
        if (status == -1)
            return -1;
        else if (status == 0)
        {
            for (int i = 0; i < totalPageNum - 1; i++)
            {
                status = appendDataInPage(fileHandle, i, data, slotSize, rid);
                if (status == -1)
                    return -1;
                else if (status == 1)
                    return 0;
            }
        }
        else if (status == 1)
        {
            status = addDataInNewPage(fileHandle, data, dataSize, rid);
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
        return -1;
    }
    int slotSize;
    int offset = slotNum; 
    memcpy(&slotSize, pageData + offset, sizeof(int));
    offset += sizeof(int);
    int dataSize = slotSize - sizeof(int);
    memcpy(data, pageData + offset, dataSize);
    delete pageData;
    return 0;
}

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) {
    int offset = 0;
    bool nullBit = false;
    int nullFieldsIndicatorActualSize = ceil((double) recordDescriptor.size() / CHAR_BIT);
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
                cout << "\t" << intValue;
            }
            else if (type == TypeReal)
            {
                float floatValue;
                memcpy(&floatValue, (char *)data + offset, sizeof(float));
                offset += sizeof(float);
                cout << "\t" << floatValue;
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
                cout << "\t" << str;
                delete str;
            }    
        }
        else
            cout << "\t";
        cout << endl;
    }
    delete nullFieldsIndicator;
    return 0;
}
