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
    return 0;
}


RC PagedFileManager::closeFile(FileHandle &fileHandle)
{
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
    return 0;
}


FileHandle::FileHandle()
{
    readPageCounter = 0;
    writePageCounter = 0;
	appendPageCounter = 0;
	file = NULL;
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
		cerr << "Page number oversizes while reading page" << endl;
#endif
		return -1;
	}
	int status = fseek(this->file, pageNum * PAGE_SIZE, SEEK_SET);
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
    ++readPageCounter;
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
	int status = fseek(this->file, pageNum * PAGE_SIZE, SEEK_SET);
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
    return 0;
}


unsigned FileHandle::getNumberOfPages()
{
	if (!this->file)
	{
#ifdef DEBUG
		cerr << "File was not open while getting the number of page" << endl;
#endif
		return -1;
	}
	int status = fseek (this->file, 0, SEEK_END);
	if (status)
	{
#ifdef DEBUG
		cerr << "fseek error in getting the number of pages" << endl;
#endif
		return -1;
	}
	long totalSize = ftell (this->file);
	if (totalSize == -1)
	{
#ifdef DEBUG
		cerr << "ftell error in getting the number of pages" << endl;
#endif
		return -1;
	}
	return totalSize / PAGE_SIZE;
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
