#include "pfm.h"

PagedFileManager* PagedFileManager::_pf_manager = 0;

PagedFileManager* PagedFileManager::instance()
{
    if(!_pf_manager)
        _pf_manager = new PagedFileManager();

    return _pf_manager;
}


PagedFileManager::PagedFileManager()
{
}


PagedFileManager::~PagedFileManager()
{
	if (_pf_manager)
		delete _pf_manager;
}


RC PagedFileManager::createFile(const string &fileName)
{
    FILE *file;
	file = fopen(fileName.c_str(), "r");
	if (file == NULL)
	{
		file = fopen(fileName.c_str(), "w");
	}
	else 
	{ 
#ifdef DEBUG
		cerr << "File exists!" << endl;
#endif
		fclose(file);
		return -1;
    }
	int status = fclose(file);
	if (status)
	{
#ifdef DEBUG
		cerr << "Cannot close the file while creating the file" << endl;
#endif
		return -1;
	}
	return 0;
}


RC PagedFileManager::destroyFile(const string &fileName)
{
    FILE *file;
	file = fopen(fileName.c_str(), "r");
	if (file == NULL)
	{
#ifdef DEBUG
		cerr << "File does not exist!" << endl;
#endif
		return -1;
	}
	else
	{
		int status = fclose(file);
		if (status)
		{
#ifdef DEBUG
			cerr << "Cannot close the file while destroying the file" << endl;
#endif
			return -1;
		}
		if (remove(fileName.c_str()) != 0)
		{
#ifdef DEBUG
			cerr << "Remove operation failed" << endl;
#endif
			return -1;
		}
#ifdef DEBUG
		else
			cout << fileName << " has been removed." << endl;
#endif
	}
	return 0;
}


RC PagedFileManager::openFile(const string &fileName, FileHandle &fileHandle)
{
	if (fileHandle.getFile())
	{
#ifdef DEBUG
		cerr << "FileHandle is already a handle for some open file!" << endl;
#endif
		return -1;
	}
    FILE *file;
	file = fopen(fileName.c_str(), "rb+");
	if (file == NULL)
	{
#ifdef DEBUG
        cerr << "File does not exist!" << endl;
#endif
		return -1;
	}
	else 
	{ 
		fileHandle.setFile(file);
    }
	fileHandle.currentFileName = fileName;
    return 0;
}


RC PagedFileManager::closeFile(FileHandle &fileHandle)
{
	fileHandle.allPagesSize.clear();
	FILE* file = fileHandle.getFile();
	if (!file)
	{
#ifdef DEBUG
		cerr << "File pointer is NULL" << endl;
#endif
		return -1;
	}
	int status = fflush(file);
	if (status)
	{
#ifdef DEBUG
		cerr << "Cannot flush the file" << endl;
#endif
		return -1;
	}
	status = fclose(file);
	if (status)
	{
#ifdef DEBUG
		cerr << "Cannot close the file while closing the file" << endl;
#endif
		return -1;
	}
	fileHandle.setFile(NULL);
	fileHandle.currentFileName = "";
    return 0;
}


FileHandle::FileHandle()
{
    readPageCounter = 0;
    writePageCounter = 0;
	appendPageCounter = 0;
	file = NULL;
	pageCount = 0;
	insertCount = 0;
}


FileHandle::~FileHandle()
{
}


RC FileHandle::readPage(PageNum pageNum, void *data)
{
	if (!this->file)
	{
#ifdef DEBUG
		cerr << "File was not open while reading page" << endl;
#endif
		return -1;
	}
	unsigned int number = getNumberOfPages();
	if (number <= pageNum)
	{
#ifdef DEBUG
		cerr << "Page number " << pageNum << " oversizes " << number << " while reading page" << endl;
#endif
		return -1;
	}
	int status = fseek(this->file, (pageNum + 1) * PAGE_SIZE, SEEK_SET);
	if (status)
	{
#ifdef DEBUG
		cerr << "fseek error in reading page " << pageNum << endl;
#endif
		return -1;
	}
	size_t readSize = fread(data, 1, PAGE_SIZE, this->file);
	if (readSize != PAGE_SIZE)
	{
#ifdef DEBUG
		cerr << "Only read " << readSize << " bytes, less than PAGE_SIZE " << PAGE_SIZE << " bytes in reading page " << pageNum << endl;
#endif
		return -1;
	}
    ++(this->readPageCounter);
	return 0;
}


RC FileHandle::writePage(PageNum pageNum, const void *data)
{
	if (!this->file)
	{
#ifdef DEBUG
		cerr << "File was not open while writing page" << endl;
#endif
		return -1;
	}
	unsigned int number = getNumberOfPages();
	if (number <= pageNum)
	{
#ifdef DEBUG
		cerr << "Page number oversizes while writing page" << endl;
#endif
		return -1;
	}
	int status = fseek(this->file, (pageNum + 1) * PAGE_SIZE, SEEK_SET);
	if (status)
	{
#ifdef DEBUG
		cerr << "fseek error in writing page " << pageNum << endl;
#endif
		return -1;
	}
	size_t writeSize = fwrite(data, 1, PAGE_SIZE, this->file);
	if (writeSize != PAGE_SIZE)
	{
#ifdef DEBUG
		cerr << "Only write " << writeSize << " bytes, less than PAGE_SIZE " << PAGE_SIZE << " bytes in writing page " << pageNum << endl;
#endif
		return -1;
	}
	status = fflush(this->file);
	if (status)
	{
#ifdef DEBUG
		cerr << "Cannot flush the file in writing page " << pageNum << endl;
#endif
		return -1;
	}
    ++writePageCounter;
	return 0;
}


RC FileHandle::appendPage(const void *data)
{
	if (!this->file)
	{
#ifdef DEBUG
		cerr << "File was not open while appending page" << endl;
#endif
		return -1;
	}
	int status = fseek(this->file, 0, SEEK_END);
	if (status)
	{
#ifdef DEBUG
		cerr << "fseek error in appending page" << endl;
#endif
		return -1;
	}
	size_t appendSize = fwrite(data, 1, PAGE_SIZE, this->file);
	if (appendSize != PAGE_SIZE)
	{
#ifdef DEBUG
		cerr << "Only append " << appendSize << " bytes, less than PAGE_SIZE " << PAGE_SIZE << " bytes in appending page" << endl;
#endif
		return -1;
	}
	++appendPageCounter;
	++(this->pageCount);
    return 0;
}


unsigned FileHandle::getNumberOfPages()
{
	return this->pageCount;
}


RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
    readPageCount = this->readPageCounter;
    writePageCount = this->writePageCounter;
    appendPageCount = this->appendPageCounter;
    return 0;
}

RC FileHandle::setFile(FILE *file)
{
    this->file = file;
    return 0;
}

FILE* FileHandle::getFile()
{
    return this->file;
}

RC FileHandle::readMetaData()
{
	if (!this->file)
	{
#ifdef DEBUG
		cerr << "File was not open while reading meta data" << endl;
#endif
		return -1;
	}
	int status = fseek(this->file, 0, SEEK_SET);
	if (status)
	{
#ifdef DEBUG
		cerr << "fseek error in reading meta data " << endl;
#endif
		return -1;
	}
	char* data = (char*)malloc(PAGE_SIZE);
	size_t readSize = fread(data, 1, PAGE_SIZE, this->file);
	if (readSize != PAGE_SIZE)
	{
#ifdef DEBUG
		cerr << "Only read " << readSize << " bytes, less than PAGE_SIZE " << PAGE_SIZE << " bytes in reading meta data " << endl;
#endif
		free(data);
		return -1;
	}

	OffsetType offset = 0;
	memcpy(&(this->readPageCounter), data + offset, sizeof(unsigned));
	offset += sizeof(unsigned);
	memcpy(&(this->writePageCounter), data + offset, sizeof(unsigned));
	offset += sizeof(unsigned);
	memcpy(&(this->appendPageCounter), data + offset, sizeof(unsigned));
	offset += sizeof(unsigned);
	memcpy(&(this->pageCount), data + offset, sizeof(unsigned));
	offset += sizeof(unsigned);
	memcpy(&(this->insertCount), data + offset, sizeof(unsigned));
	offset += sizeof(unsigned);
	memcpy(&(this->currentVersion), data + offset, sizeof(unsigned));
	offset += sizeof(unsigned);
	free(data);
	return 0;
}

RC FileHandle::writeMetaData()
{
	if (!this->file)
	{
#ifdef DEBUG
		cerr << "File was not open while writing meta data" << endl;
#endif
		return -1;
	}
	int status = fseek(this->file, 0, SEEK_SET);
	if (status)
	{
#ifdef DEBUG
		cerr << "fseek error in writing meta data " << endl;
#endif
		return -1;
	}
	char* data = (char*)calloc(PAGE_SIZE, 1);
	OffsetType offset = 0;
	memcpy(data + offset, &(this->readPageCounter), sizeof(unsigned));
	offset += sizeof(unsigned);
	memcpy(data + offset, &(this->writePageCounter), sizeof(unsigned));
	offset += sizeof(unsigned);
	memcpy(data + offset, &(this->appendPageCounter), sizeof(unsigned));
	offset += sizeof(unsigned);
	memcpy(data + offset, &(this->pageCount), sizeof(unsigned));
	offset += sizeof(unsigned);
	memcpy(data + offset, &(this->insertCount), sizeof(unsigned));
	offset += sizeof(unsigned);
	memcpy(data + offset, &(this->currentVersion), sizeof(unsigned));
	offset += sizeof(unsigned);
	size_t writeSize = fwrite(data, 1, PAGE_SIZE, this->file);
	if (writeSize != PAGE_SIZE)
	{
#ifdef DEBUG
		cerr << "Only write " << writeSize << " bytes, less than PAGE_SIZE " << PAGE_SIZE << " bytes in writing meta data " << endl;
#endif
		free(data);
		return -1;
	}
	status = fflush(this->file);
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

RC FileHandle::generateAllPagesSize(vector<OffsetType> &allPagesSize)
{
	PageNum pageCountInAllPagesSize = allPagesSize.size();
	for (PageNum i = 0; i < this->pageCount; i++)
	{
		int status = fseek(this->file, (i + 1) * PAGE_SIZE, SEEK_SET);
		if (status)
		{
#ifdef DEBUG
			cerr << "fseek error in reading meta data " << endl;
#endif
			return -1;
		}
		int pageSize = 0;
		fread(&pageSize, 1, sizeof(OffsetType), this->file);
		if (i < pageCountInAllPagesSize)
			allPagesSize[i] = pageSize;
		else
			allPagesSize.push_back(pageSize);
	}
	return 0;
}

MarkType FileHandle::getCurrentVersion()
{
	return this->currentVersion;
}