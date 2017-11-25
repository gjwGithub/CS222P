
#include "rm.h"

RelationManager* RelationManager::instance()
{
	static RelationManager _rm;
	return &_rm;
}

RelationManager::RelationManager()
{
}

RelationManager::~RelationManager()
{
	if (fh_table.currentFileName != "")
		fm_table->closeFile(fh_table);
}

RC RelationManager::createCatalog()
{
	vector<Attribute> attrs;

	Attribute attr;
	attr.name = "table-id";
	attr.type = TypeInt;
	attr.length = (AttrLength)4;
	attrs.push_back(attr);

	attr.name = "table-name";
	attr.type = TypeVarChar;
	attr.length = (AttrLength)50;
	attrs.push_back(attr);

	attr.name = "file-name";
	attr.type = TypeVarChar;
	attr.length = (AttrLength)50;
	attrs.push_back(attr);

	attr.name = "isSys";
	attr.type = TypeInt;
	attr.length = (AttrLength)4;
	attrs.push_back(attr);

	string tableName = "Tables";
	fm_table->createFile(tableName);
	fm_table->openFile("Tables", fh_table);
	/*int pageNumber = fh_table.getNumberOfPages();
	int sum_tables = 0;
	int table_ID = 0;
	for (int i = 0; i < pageNumber; i++) {
		void *buffer = malloc(PAGE_SIZE);
		fh_table.readPage(i, buffer);
		short sig_count = 0;
		memcpy(&sig_count, (char *)buffer + PAGE_SIZE - 2, 2);
		sum_tables = sum_tables + sig_count;
		free(buffer);
	}
	table_ID = sum_tables;*/
	void *tuple = malloc(200);
	int nullAttributesIndicatorActualSize = ceil((double)4 / CHAR_BIT);
	unsigned char *nullsIndicator = (unsigned char *)malloc(nullAttributesIndicatorActualSize);
	memset(nullsIndicator, 0, nullAttributesIndicatorActualSize); int tupleSize = 0;
	prepareTuple_tables(4, nullsIndicator, 0, tableName.size(), tableName, tableName.size(), tableName, 1, tuple, &tupleSize);
	free(nullsIndicator);

	RID rid;
	fm_table->insertRecord(fh_table, attrs, tuple, rid);
	free(tuple);

	tableName = "Columns";
	tuple = malloc(200);
	nullAttributesIndicatorActualSize = ceil((double)4 / CHAR_BIT);
	nullsIndicator = (unsigned char *)malloc(nullAttributesIndicatorActualSize);
	memset(nullsIndicator, 0, nullAttributesIndicatorActualSize); tupleSize = 0;
	prepareTuple_tables(4, nullsIndicator, 1, tableName.size(), tableName, tableName.size(), tableName, 1, tuple, &tupleSize);
	free(nullsIndicator);

	fm_table->insertRecord(fh_table, attrs, tuple, rid);
	free(tuple);
	fm_table->closeFile(fh_table);

	//createTable("usr_catalog_tables",attrs);

	vector<Attribute> attrs1;

	Attribute attr1;
	attr1.name = "table-id";
	attr1.type = TypeInt;
	attr1.length = (AttrLength)4;
	attrs1.push_back(attr1);

	attr1.name = "column-name";
	attr1.type = TypeVarChar;
	attr1.length = (AttrLength)50;
	attrs1.push_back(attr1);

	attr1.name = "column-type";
	attr1.type = TypeInt;
	attr1.length = (AttrLength)4;
	attrs1.push_back(attr1);

	attr1.name = "column-length";
	attr1.type = TypeInt;
	attr1.length = (AttrLength)4;
	attrs1.push_back(attr1);

	attr1.name = "column-position";
	attr1.type = TypeInt;
	attr1.length = (AttrLength)4;
	attrs1.push_back(attr1);

	attr1.name = "startVersion";
	attr1.type = TypeInt;
	attr1.length = (AttrLength)4;
	attrs1.push_back(attr1);

	attr1.name = "endVersion";
	attr1.type = TypeInt;
	attr1.length = (AttrLength)4;
	attrs1.push_back(attr1);

	fm_table->createFile("Columns");
	fm_table->openFile("Columns", fh_table);

	for (size_t i = 0; i < attrs.size(); i++) {
		void *tuple1 = malloc(200);
		int nullAttributesIndicatorActualSize1 = ceil((double)7 / CHAR_BIT);
		unsigned char *nullsIndicator1 = (unsigned char *)malloc(nullAttributesIndicatorActualSize1);
		memset(nullsIndicator1, 0, nullAttributesIndicatorActualSize1);
		tupleSize = 0;
		prepareTuple_cols(7, nullsIndicator1, 0, attrs[i].name.size(), attrs[i].name, attrs[i].type, attrs[i].length, i + 1, 0, 0, tuple1, &tupleSize);
		fm_table->insertRecord(fh_table, attrs1, tuple1, rid);
		free(tuple1);
		free(nullsIndicator1);
	}

	for (size_t i = 0; i < attrs1.size(); i++) {
		void *tuple1 = malloc(200);
		int nullAttributesIndicatorActualSize1 = ceil((double)7 / CHAR_BIT);
		unsigned char *nullsIndicator1 = (unsigned char *)malloc(nullAttributesIndicatorActualSize1);
		memset(nullsIndicator1, 0, nullAttributesIndicatorActualSize1);
		tupleSize = 0;
		prepareTuple_cols(7, nullsIndicator1, 1, attrs1[i].name.size(), attrs1[i].name, attrs1[i].type, attrs1[i].length, i + 1, 0, 0, tuple1, &tupleSize);
		fm_table->insertRecord(fh_table, attrs1, tuple1, rid);
		free(tuple1);
		free(nullsIndicator1);
	}
	fm_table->closeFile(fh_table);
	return 0;
}

RC RelationManager::deleteCatalog()
{

	fm_table->destroyFile("Tables");
	fm_table->destroyFile("Columns");
	return 0;
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
	if (tableName == "Tables" || tableName == "Columns")
		return -1;
	int tupleSize = 0;
	RC rel11 = fm_table->createFile(tableName);
	if (rel11 == -1)
		return -1;
	vector<Attribute> attrs0;

	Attribute attr0;
	attr0.name = "table-id";
	attr0.type = TypeInt;
	attr0.length = (AttrLength)4;
	attrs0.push_back(attr0);

	attr0.name = "table-name";
	attr0.type = TypeVarChar;
	attr0.length = (AttrLength)50;
	attrs0.push_back(attr0);

	attr0.name = "file-name";
	attr0.type = TypeVarChar;
	attr0.length = (AttrLength)50;
	attrs0.push_back(attr0);
	attr0.name = "isSys";
	attr0.type = TypeInt;
	attr0.length = (AttrLength)4;
	attrs0.push_back(attr0);
	fm_table->openFile("Tables", fh_table);
	int table_ID = fh_table.insertCount;
	void *tuple = malloc(200);
	int nullAttributesIndicatorActualSize = ceil((double)4 / CHAR_BIT);
	unsigned char *nullsIndicator = (unsigned char *)malloc(nullAttributesIndicatorActualSize);
	memset(nullsIndicator, 0, nullAttributesIndicatorActualSize);
	prepareTuple_tables(4, nullsIndicator, table_ID, tableName.size(), tableName, tableName.size(), tableName, 0, tuple, &tupleSize);
	free(nullsIndicator);

	RID rid;
	fm_table->insertRecord(fh_table, attrs0, tuple, rid);
	free(tuple);
	fm_table->closeFile(fh_table);
	vector<Attribute> attrs1;

	Attribute attr1;
	attr1.name = "table-id";
	attr1.type = TypeInt;
	attr1.length = (AttrLength)4;
	attrs1.push_back(attr1);

	attr1.name = "column-name";
	attr1.type = TypeVarChar;
	attr1.length = (AttrLength)50;
	attrs1.push_back(attr1);

	attr1.name = "column-type";
	attr1.type = TypeInt;
	attr1.length = (AttrLength)4;
	attrs1.push_back(attr1);

	attr1.name = "column-length";
	attr1.type = TypeInt;
	attr1.length = (AttrLength)4;
	attrs1.push_back(attr1);

	attr1.name = "column-position";
	attr1.type = TypeInt;
	attr1.length = (AttrLength)4;
	attrs1.push_back(attr1);

	attr1.name = "startVersion";
	attr1.type = TypeInt;
	attr1.length = (AttrLength)4;
	attrs1.push_back(attr1);

	attr1.name = "endVersion";
	attr1.type = TypeInt;
	attr1.length = (AttrLength)4;
	attrs1.push_back(attr1);

	fm_table->openFile("Columns", fh_table);
	for (size_t i = 0; i < attrs.size(); i++) {
		void *tuple1 = malloc(200);
		int nullAttributesIndicatorActualSize1 = ceil((double)7 / CHAR_BIT);
		unsigned char *nullsIndicator1 = (unsigned char *)malloc(nullAttributesIndicatorActualSize1);
		memset(nullsIndicator1, 0, nullAttributesIndicatorActualSize1);
		tupleSize = 0;
		prepareTuple_cols(7, nullsIndicator1, table_ID, attrs[i].name.size(), attrs[i].name, attrs[i].type, attrs[i].length, i + 1, 0, 0, tuple1, &tupleSize);
		fm_table->insertRecord(fh_table, attrs1, tuple1, rid);
		free(tuple1);
		free(nullsIndicator1);
	}
	fm_table->closeFile(fh_table);
	return 0;
}

RC RelationManager::deleteTable(const string &tableName)
{
	if (tableName == "Tables" || tableName == "Columns")
		return -1;
	auto ite = map_attributes.find(tableName);
	if (ite != map_attributes.end()) {
		map_attributes.erase(ite);
	}
	RC rel11 = fm_table->destroyFile(tableName);
	if (rel11 == -1)
		return -1;
	string attr = "table-id";
	vector<string> attributes;
	attributes.push_back(attr);
	RM_ScanIterator rmsi;
	int size = tableName.size();
	void *data1 = malloc(size + 4);
	memcpy((char *)data1, &size, 4);
	memcpy((char *)data1 + 4, tableName.c_str(), size);
	scan("Tables", "table-name", EQ_OP, data1, attributes, rmsi);
	free(data1);
	int nullAttributesIndicatorActualSize1 = ceil((double)4 / CHAR_BIT);
	RID rid;
	void *returnedData = malloc(200);
	rmsi.getNextTuple(rid, returnedData);
	int tableID = *(int *)((char *)returnedData + nullAttributesIndicatorActualSize1);
	fm_table->closeFile(fh_table);
	rmsi.close();
	RC qq1 = deleteTuple("Tables", rid);  //delete
	if (qq1 == -1)
		return -1;

	vector<string> attribute1;
	attribute1.push_back("column-name");
	attribute1.push_back("column-type");
	attribute1.push_back("column-length");
	void *data = malloc(4);
	memcpy((char *)data, &tableID, 4);
	scan("Columns", "table-id", EQ_OP, data, attribute1, rmsi);
	free(data);
	returnedData = realloc(returnedData, 200);

	vector<RID> vector_rid;
	while (rmsi.getNextTuple(rid, returnedData) != RM_EOF)
	{
		vector_rid.push_back(rid);
	}
	free(returnedData);
	fm_table->closeFile(fh_table);
	rmsi.close();

	for (size_t i = 0; i < vector_rid.size(); i++) {
		RC qq2 = deleteTuple("Columns", vector_rid[i]);
		if (qq2 == -1)
			return -1;
	}
	return 0;
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
	auto ite = map_attributes.find(tableName);
	if (ite != map_attributes.end())
	{
		attrs = ite->second;
		return 0;
	}
	
	FILE* file;
	file = fopen(tableName.c_str(), "rb+");
	if (file == NULL) {
		return -1;
	}
	else {
		fclose(file);
	}
	string attr = "table-id";
	vector<string> attributes;
	attributes.push_back(attr);
	RM_ScanIterator rmsi;
	int size = tableName.size();
	void *data1 = malloc(size + 4);
	memcpy((char *)data1, &size, 4);
	memcpy((char *)data1 + 4, tableName.c_str(), size);
	scan("Tables", "table-name", EQ_OP, data1, attributes, rmsi);
	free(data1);
	int nullAttributesIndicatorActualSize1 = ceil((double)4 / CHAR_BIT);
	RID rid;
	void *returnedData = malloc(200);
	rmsi.getNextTuple(rid, returnedData);
	int tableID = *(int *)((char *)returnedData + nullAttributesIndicatorActualSize1);
	fm_table->closeFile(fh_table);
	rmsi.close();

	vector<string> attribute1;
	attribute1.push_back("column-name");
	attribute1.push_back("column-type");
	attribute1.push_back("column-length");
	attribute1.push_back("endVersion");
	int nullAttributesIndicatorActualSize = ceil((double)7 / CHAR_BIT);
	void *data = malloc(4);
	memcpy((char *)data, &tableID, 4);
	scan("Columns", "table-id", EQ_OP, data, attribute1, rmsi);

	fm_table->openFile(tableName, fh_table);
	int currentVersion = (int)fh_table.currentVersion;
	fm_table->closeFile(fh_table);
	fm_table->openFile("Columns", fh_table);

	free(data);
	returnedData = realloc(returnedData, 200);
	while (rmsi.getNextTuple(rid, returnedData) != RM_EOF)
	{
		Attribute attr1;
		int offset = 0;
		int nameSize = *(int *)((char *)returnedData + offset + nullAttributesIndicatorActualSize);
		offset += 4;
		char* fieldName = (char *)malloc(nameSize + 1);
		memcpy(fieldName, (char *)returnedData + nullAttributesIndicatorActualSize + offset, nameSize);
		fieldName[nameSize] = '\0';
		offset += nameSize;
		string stringfieldName = fieldName;
		attr1.name = stringfieldName;
		int fieldType = *(int *)((char *)returnedData + offset + nullAttributesIndicatorActualSize);
		offset += 4;
		attr1.type = (AttrType)fieldType;
		int maxLength = *(int *)((char *)returnedData + offset + nullAttributesIndicatorActualSize);
		attr1.length = (AttrLength)maxLength;

		offset += sizeof(int);
		int endVersion = *(int *)((char *)returnedData + offset + nullAttributesIndicatorActualSize);
		if (endVersion == currentVersion)
			attrs.push_back(attr1);
		free(fieldName);
	}
	free(returnedData);
	fm_table->closeFile(fh_table);
	rmsi.close();

	map_attributes.insert(pair<string, vector<Attribute>>(tableName, attrs));

	return 0;
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
	if (tableName == "Tables" || tableName == "Columns")
		return -1;
	vector<Attribute> attributes;
	RC rel1 = getAttributes(tableName, attributes);
	if (rel1 == -1)
		return -1;

	if (tableName != fh_table.currentFileName)
	{
		if (fh_table.currentFileName != "")
			fm_table->closeFile(fh_table);
		fm_table->openFile(tableName, fh_table);
	}
	RC rel = fm_table->insertRecord(fh_table, attributes, data, rid);
	return rel;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
	vector<Attribute> attributes;
	RC rel1 = getAttributes(tableName, attributes);
	if (rel1 == -1)
		return -1;

	if (tableName != fh_table.currentFileName)
	{
		if (fh_table.currentFileName != "")
			fm_table->closeFile(fh_table);
		fm_table->openFile(tableName, fh_table);
	}
	RC rel = fm_table->deleteRecord(fh_table, attributes, rid);
	return rel;
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid)
{
	vector<Attribute> attributes;
	RC rel1 = getAttributes(tableName, attributes);
	if (rel1 == -1)
		return -1;

	if (tableName != fh_table.currentFileName)
	{
		if (fh_table.currentFileName != "")
			fm_table->closeFile(fh_table);
		fm_table->openFile(tableName, fh_table);
	}
	RC rel = fm_table->updateRecord(fh_table, attributes, data, rid);
	return rel;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
	if (tableName == "Tables" || tableName == "Columns")
		return -1;
	vector<Attribute> attributes;
	RC rel1 = getAttributes(tableName, attributes);
	if (rel1 == -1)
		return -1;

	this->versionTable = generateVersionTable(tableName);
	if (tableName != fh_table.currentFileName)
	{
		if (fh_table.currentFileName != "")
			fm_table->closeFile(fh_table);
		fm_table->openFile(tableName, fh_table);
	}
	RC rel = fm_table->readRecord(fh_table, attributes, rid, data);
	return rel;
}

RC RelationManager::printTuple(const vector<Attribute> &attrs, const void *data)
{

	return fm_table->printRecord(attrs, data);
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{
	if (tableName == "Tables" || tableName == "Columns")
		return -1;
	vector<Attribute> attributes;
	RC rel1 = getAttributes(tableName, attributes);
	if (rel1 == -1)
		return -1;

	this->versionTable = generateVersionTable(tableName);
	if (tableName != fh_table.currentFileName)
	{
		if (fh_table.currentFileName != "")
			fm_table->closeFile(fh_table);
		fm_table->openFile(tableName, fh_table);
	}
	RC rel = fm_table->readAttribute(fh_table, attributes, rid, attributeName, data);
	return rel;
}

RC RelationManager::scan(const string &tableName,
	const string &conditionAttribute,
	const CompOp compOp,
	const void *value,
	const vector<string> &attributeNames,
	RM_ScanIterator &rm_ScanIterator)
{
	if (tableName == "Tables") {
		vector<Attribute> attrs0;
		Attribute attr0;
		attr0.name = "table-id";
		attr0.type = TypeInt;
		attr0.length = (AttrLength)4;
		attrs0.push_back(attr0);

		attr0.name = "table-name";
		attr0.type = TypeVarChar;
		attr0.length = (AttrLength)50;
		attrs0.push_back(attr0);

		attr0.name = "file-name";
		attr0.type = TypeVarChar;
		attr0.length = (AttrLength)50;
		attrs0.push_back(attr0);

		attr0.name = "isSys";
		attr0.type = TypeInt;
		attr0.length = (AttrLength)4;
		attrs0.push_back(attr0);


		fm_table->openFile("Tables", fh_table);
		fm_table->scan(fh_table, attrs0, conditionAttribute, compOp, value, attributeNames, rm_ScanIterator.rbfm_ScanIterator);
		//fm_table->closeFile(fh_table);

	}
	else if (tableName == "Columns") {
		vector<Attribute> attrs1;

		Attribute attr1;
		attr1.name = "table-id";
		attr1.type = TypeInt;
		attr1.length = (AttrLength)4;
		attrs1.push_back(attr1);

		attr1.name = "column-name";
		attr1.type = TypeVarChar;
		attr1.length = (AttrLength)50;
		attrs1.push_back(attr1);

		attr1.name = "column-type";
		attr1.type = TypeInt;
		attr1.length = (AttrLength)4;
		attrs1.push_back(attr1);

		attr1.name = "column-length";
		attr1.type = TypeInt;
		attr1.length = (AttrLength)4;
		attrs1.push_back(attr1);

		attr1.name = "column-position";
		attr1.type = TypeInt;
		attr1.length = (AttrLength)4;
		attrs1.push_back(attr1);

		attr1.name = "startVersion";
		attr1.type = TypeInt;
		attr1.length = (AttrLength)4;
		attrs1.push_back(attr1);

		attr1.name = "endVersion";
		attr1.type = TypeInt;
		attr1.length = (AttrLength)4;
		attrs1.push_back(attr1);

		fm_table->openFile("Columns", fh_table);
		fm_table->scan(fh_table, attrs1, conditionAttribute, compOp, value, attributeNames, rm_ScanIterator.rbfm_ScanIterator);
		//fm_table->closeFile(fh_table);

	}
	else {
		vector<Attribute> attrs;
		getAttributes(tableName, attrs);
		fm_table->openFile(tableName, fh_table);
		fm_table->scan(fh_table, attrs, conditionAttribute, compOp, value, attributeNames, rm_ScanIterator.rbfm_ScanIterator);

		//fm_table->closeFile(fh_table);
	}
	vector<vector<Attribute>> versionTable = generateVersionTable(tableName);
	rm_ScanIterator.rbfm_ScanIterator.setVersionTable(versionTable);
	fm_table->openFile(tableName, fh_table);
	return 0;
}

// Extra credit work
RC RelationManager::dropAttribute(const string &tableName, const string &attributeName)
{
	fm_table->openFile(tableName, fh_table);
	int currentVersion = (int)++fh_table.currentVersion;
	fh_table.writeMetaData();
	fm_table->closeFile(fh_table);

	string attr = "table-id";
	vector<string> attributes;
	attributes.push_back(attr);
	RM_ScanIterator rmsi;
	int size = tableName.size();
	void *data1 = malloc(size + 4);
	memcpy((char *)data1, &size, 4);
	memcpy((char *)data1 + 4, tableName.c_str(), size);
	scan("Tables", "table-name", EQ_OP, data1, attributes, rmsi);
	free(data1);
	int nullAttributesIndicatorActualSize1 = ceil((double)4 / CHAR_BIT);
	RID rid;
	void *returnedData = malloc(200);
	rmsi.getNextTuple(rid, returnedData);
	int tableID = *(int *)((char *)returnedData + nullAttributesIndicatorActualSize1);
	fm_table->closeFile(fh_table);
	rmsi.close();

	vector<string> attribute1;
	attribute1.push_back("column-name");
	attribute1.push_back("column-type");
	attribute1.push_back("column-length");
	attribute1.push_back("column-position");
	attribute1.push_back("startVersion");
	attribute1.push_back("endVersion");
	int nullAttributesIndicatorActualSize = ceil((double)6 / CHAR_BIT);
	void *data = malloc(4);
	memcpy((char *)data, &tableID, 4);
	scan("Columns", "table-id", EQ_OP, data, attribute1, rmsi);
	free(data);
	returnedData = realloc(returnedData, 200);
	while (rmsi.getNextTuple(rid, returnedData) != RM_EOF)
	{
		Attribute attr1;
		int offset = 0;
		int nameSize = *(int *)((char *)returnedData + offset + nullAttributesIndicatorActualSize);
		offset += 4;
		char* fieldName = (char *)malloc(nameSize + 1);
		memcpy(fieldName, (char *)returnedData + nullAttributesIndicatorActualSize + offset, nameSize);
		fieldName[nameSize] = '\0';
		offset += nameSize;
		string stringfieldName = fieldName;
		attr1.name = stringfieldName;
		int fieldType = *(int *)((char *)returnedData + offset + nullAttributesIndicatorActualSize);
		offset += 4;
		attr1.type = (AttrType)fieldType;
		int maxLength = *(int *)((char *)returnedData + offset + nullAttributesIndicatorActualSize);
		attr1.length = (AttrLength)maxLength;
		offset += sizeof(int);
		int columnID = *(int *)((char *)returnedData + offset + nullAttributesIndicatorActualSize);
		offset += sizeof(int);
		int startVersion = *(int *)((char *)returnedData + offset + nullAttributesIndicatorActualSize);
		offset += sizeof(int);
		int endVersion = *(int *)((char *)returnedData + offset + nullAttributesIndicatorActualSize);

		if (stringfieldName != attributeName && endVersion == currentVersion - 1)
		{
			void *tuple = malloc(200);
			int nullAttributesIndicatorActualSize2 = ceil((double)7 / CHAR_BIT);
			unsigned char *nullsIndicator = (unsigned char *)malloc(nullAttributesIndicatorActualSize2);
			memset(nullsIndicator, 0, nullAttributesIndicatorActualSize2);
			int tupleSize = 0;
			prepareTuple_cols(7, nullsIndicator, tableID, attr1.name.size(), attr1.name, attr1.type, attr1.length, columnID, startVersion, currentVersion, tuple, &tupleSize);

			vector<Attribute> attributes;
			RC rel1 = getAttributes("Columns", attributes);
			if (rel1 == -1)
				return -1;
			fm_table->openFile("Columns", fh_table);
			RC rel = fm_table->updateRecord(fh_table, attributes, tuple, rid);
			if (rel == -1)
				return -1;
			fm_table->closeFile(fh_table);

			fm_table->openFile("Columns", fh_table);

			free(nullsIndicator);
			free(tuple);
		}
		free(fieldName);
	}
	free(returnedData);
	fm_table->closeFile(fh_table);
	rmsi.close();

	auto ite = map_attributes.find(tableName);
	if (ite != map_attributes.end())
	{
		for (auto i = ite->second.begin(); i != ite->second.end(); i++)
		{
			if (i->name == attributeName)
			{
				ite->second.erase(i);
				break;
			}
		}
		auto ite2 = map_versionTable.find(tableName);
		if (ite2 != map_versionTable.end())
			ite2->second.push_back(ite->second);
	}
	else
	{
		vector<Attribute> attrs;
		if (getAttributes(tableName, attrs) == -1)
			return -1;
		auto ite2 = map_versionTable.find(tableName);
		if (ite2 != map_versionTable.end())
			ite2->second.push_back(attrs);
	}

	return 0;
}

// Extra credit work
RC RelationManager::addAttribute(const string &tableName, const Attribute &attr)
{
	fm_table->openFile(tableName, fh_table);
	int currentVersion = (int)++fh_table.currentVersion;
	fh_table.writeMetaData();
	fm_table->closeFile(fh_table);

	string attrStr = "table-id";
	vector<string> attributes;
	attributes.push_back(attrStr);
	RM_ScanIterator rmsi;
	int size = tableName.size();
	void *data1 = malloc(size + 4);
	memcpy((char *)data1, &size, 4);
	memcpy((char *)data1 + 4, tableName.c_str(), size);
	scan("Tables", "table-name", EQ_OP, data1, attributes, rmsi);
	free(data1);
	int nullAttributesIndicatorActualSize1 = ceil((double)4 / CHAR_BIT);
	RID rid;
	void *returnedData = malloc(200);
	rmsi.getNextTuple(rid, returnedData);
	int tableID = *(int *)((char *)returnedData + nullAttributesIndicatorActualSize1);
	fm_table->closeFile(fh_table);
	rmsi.close();

	vector<string> attribute1;
	attribute1.push_back("column-name");
	attribute1.push_back("column-type");
	attribute1.push_back("column-length");
	attribute1.push_back("column-position");
	attribute1.push_back("startVersion");
	attribute1.push_back("endVersion");
	int nullAttributesIndicatorActualSize = ceil((double)6 / CHAR_BIT);
	void *data = malloc(4);
	memcpy((char *)data, &tableID, 4);
	scan("Columns", "table-id", EQ_OP, data, attribute1, rmsi);
	free(data);
	returnedData = realloc(returnedData, 200);
	int fieldCount = 0;
	while (rmsi.getNextTuple(rid, returnedData) != RM_EOF)
	{
		Attribute attr1;
		int offset = 0;
		int nameSize = *(int *)((char *)returnedData + offset + nullAttributesIndicatorActualSize);
		offset += 4;
		char* fieldName = (char *)malloc(nameSize + 1);
		memcpy(fieldName, (char *)returnedData + nullAttributesIndicatorActualSize + offset, nameSize);
		fieldName[nameSize] = '\0';
		offset += nameSize;
		string stringfieldName = fieldName;
		attr1.name = stringfieldName;
		int fieldType = *(int *)((char *)returnedData + offset + nullAttributesIndicatorActualSize);
		offset += 4;
		attr1.type = (AttrType)fieldType;
		int maxLength = *(int *)((char *)returnedData + offset + nullAttributesIndicatorActualSize);
		attr1.length = (AttrLength)maxLength;
		offset += sizeof(int);
		int columnID = *(int *)((char *)returnedData + offset + nullAttributesIndicatorActualSize);
		offset += sizeof(int);
		int startVersion = *(int *)((char *)returnedData + offset + nullAttributesIndicatorActualSize);
		offset += sizeof(int);
		int endVersion = *(int *)((char *)returnedData + offset + nullAttributesIndicatorActualSize);

		if (endVersion == currentVersion - 1)
		{
			void *tuple = malloc(200);
			int nullAttributesIndicatorActualSize2 = ceil((double)7 / CHAR_BIT);
			unsigned char *nullsIndicator = (unsigned char *)malloc(nullAttributesIndicatorActualSize2);
			memset(nullsIndicator, 0, nullAttributesIndicatorActualSize2);
			int tupleSize = 0;
			prepareTuple_cols(7, nullsIndicator, tableID, attr1.name.size(), attr1.name, attr1.type, attr1.length, columnID, startVersion, currentVersion, tuple, &tupleSize);

			vector<Attribute> attributes;
			RC rel1 = getAttributes("Columns", attributes);
			if (rel1 == -1)
				return -1;
			fm_table->openFile("Columns", fh_table);
			RC rel = fm_table->updateRecord(fh_table, attributes, tuple, rid);
			if (rel == -1)
				return -1;
			fm_table->closeFile(fh_table);

			fm_table->openFile("Columns", fh_table);

			free(nullsIndicator);
			free(tuple);
		}

		free(fieldName);
		fieldCount++;
	}
	free(returnedData);
	fm_table->closeFile(fh_table);
	rmsi.close();

	void *tuple = malloc(200);
	int nullAttributesIndicatorActualSize2 = ceil((double)7 / CHAR_BIT);
	unsigned char *nullsIndicator = (unsigned char *)malloc(nullAttributesIndicatorActualSize2);
	memset(nullsIndicator, 0, nullAttributesIndicatorActualSize2);
	int tupleSize = 0;
	prepareTuple_cols(7, nullsIndicator, tableID, attr.name.size(), attr.name, attr.type, attr.length, fieldCount + 1, currentVersion, currentVersion, tuple, &tupleSize);

	vector<Attribute> attributes2;
	RC rel1 = getAttributes("Columns", attributes2);
	if (rel1 == -1)
		return -1;
	fm_table->openFile("Columns", fh_table);
	RC rel = fm_table->insertRecord(fh_table, attributes2, tuple, rid);
	if (rel == -1)
		return -1;
	fm_table->closeFile(fh_table);
	free(nullsIndicator);
	free(tuple);

	auto ite = map_attributes.find(tableName);
	if (ite != map_attributes.end())
	{
		ite->second.push_back(attr);
		auto ite2 = map_versionTable.find(tableName);
		if (ite2 != map_versionTable.end())
			ite2->second.push_back(ite->second);
	}
	else
	{
		vector<Attribute> attrs;
		if (getAttributes(tableName, attrs) == -1)
			return -1;
		auto ite2 = map_versionTable.find(tableName);
		if (ite2 != map_versionTable.end())
			ite2->second.push_back(attrs);
	}
	return 0;
}

void RelationManager::prepareTuple_tables(int attributeCount, unsigned char *nullAttributesIndicator, const int Table_ID, const int lengthOftable, const string &tableName, const int lengthOffile, const string &fileName, const int isSys, void *buffer, int *tupleSize)
{
	int offset = 0;

	// Null-indicators
	bool nullBit = false;
	int nullAttributesIndicatorActualSize = ceil((double)attributeCount / CHAR_BIT);

	// Null-indicator for the fields
	memcpy((char *)buffer + offset, nullAttributesIndicator, nullAttributesIndicatorActualSize);
	offset += nullAttributesIndicatorActualSize;

	// Beginning of the actual data    
	// Note that the left-most bit represents the first field. Thus, the offset is 7 from right, not 0.
	// e.g., if a tuple consists of four attributes and they are all nulls, then the bit representation will be: [11110000]

	// Is the table-ID field not-NULL?
	nullBit = nullAttributesIndicator[0] & (1 << 7);

	if (!nullBit) {
		memcpy((char *)buffer + offset, &Table_ID, sizeof(int));
		offset += sizeof(int);
	}

	// Is the age field not-NULL?
	nullBit = nullAttributesIndicator[0] & (1 << 6);

	if (!nullBit) {
		memcpy((char *)buffer + offset, &lengthOftable, sizeof(int));
		offset += sizeof(int);
		memcpy((char *)buffer + offset, tableName.c_str(), lengthOftable);
		offset += lengthOftable;
	}


	// Is the height field not-NULL?
	nullBit = nullAttributesIndicator[0] & (1 << 5);

	if (!nullBit) {
		memcpy((char *)buffer + offset, &lengthOffile, sizeof(int));
		offset += sizeof(int);
		memcpy((char *)buffer + offset, fileName.c_str(), lengthOffile);
		offset += lengthOffile;
	}


	// Is the isSys field not-NULL?
	nullBit = nullAttributesIndicator[0] & (1 << 4);

	if (!nullBit) {
		memcpy((char *)buffer + offset, &isSys, sizeof(int));
		offset += sizeof(int);
	}

	*tupleSize = offset;
}

void RelationManager::prepareTuple_cols(int attributeCount, unsigned char *nullAttributesIndicator, const int Table_ID, const int lengthOffield, const string &fieldName, const int fieldOfType, const int max_length, const int field_ID, const int startVersion, const int endVersion, void *buffer, int *tupleSize)
{
	int offset = 0;

	// Null-indicators
	bool nullBit = false;
	int nullAttributesIndicatorActualSize = ceil((double)attributeCount / CHAR_BIT);

	// Null-indicator for the fields
	memcpy((char *)buffer + offset, nullAttributesIndicator, nullAttributesIndicatorActualSize);
	offset += nullAttributesIndicatorActualSize;

	// Beginning of the actual data    
	// Note that the left-most bit represents the first field. Thus, the offset is 7 from right, not 0.
	// e.g., if a tuple consists of four attributes and they are all nulls, then the bit representation will be: [11110000]

	// Is the table-ID field not-NULL?
	nullBit = nullAttributesIndicator[0] & (1 << 7);

	if (!nullBit) {
		memcpy((char *)buffer + offset, &Table_ID, sizeof(int));
		offset += sizeof(int);
	}

	// Is the field name field not-NULL?
	nullBit = nullAttributesIndicator[0] & (1 << 6);

	if (!nullBit) {
		memcpy((char *)buffer + offset, &lengthOffield, sizeof(int));
		offset += sizeof(int);
		memcpy((char *)buffer + offset, fieldName.c_str(), lengthOffield);
		offset += lengthOffield;
	}


	// Is the type field not-NULL?
	nullBit = nullAttributesIndicator[0] & (1 << 5);

	if (!nullBit) {
		memcpy((char *)buffer + offset, &fieldOfType, sizeof(int));
		offset += sizeof(int);
	}


	// Is the isSys field not-NULL?
	nullBit = nullAttributesIndicator[0] & (1 << 4);

	if (!nullBit) {
		memcpy((char *)buffer + offset, &max_length, sizeof(int));
		offset += sizeof(int);
	}

	nullBit = nullAttributesIndicator[0] & (1 << 3);

	if (!nullBit) {
		memcpy((char *)buffer + offset, &field_ID, sizeof(int));
		offset += sizeof(int);
	}

	nullBit = nullAttributesIndicator[0] & (1 << 2);
	if (!nullBit) {
		memcpy((char *)buffer + offset, &startVersion, sizeof(int));
		offset += sizeof(int);
	}

	nullBit = nullAttributesIndicator[0] & (1 << 1);
	if (!nullBit) {
		memcpy((char *)buffer + offset, &endVersion, sizeof(int));
		offset += sizeof(int);
	}

	*tupleSize = offset;
}

vector<vector<Attribute>> RelationManager::generateVersionTable(const string &tableName)
{
	auto ite = map_versionTable.find(tableName);
	if (ite != map_versionTable.end())
	{
		return ite->second;
	}

	if (tableName == "Tables")
	{
		vector<vector<Attribute>> result;
		vector<Attribute> attrs0;
		Attribute attr0;
		attr0.name = "table-id";
		attr0.type = TypeInt;
		attr0.length = (AttrLength)4;
		attrs0.push_back(attr0);

		attr0.name = "table-name";
		attr0.type = TypeVarChar;
		attr0.length = (AttrLength)50;
		attrs0.push_back(attr0);

		attr0.name = "file-name";
		attr0.type = TypeVarChar;
		attr0.length = (AttrLength)50;
		attrs0.push_back(attr0);

		attr0.name = "isSys";
		attr0.type = TypeInt;
		attr0.length = (AttrLength)4;
		attrs0.push_back(attr0);

		result.push_back(attrs0);

		map_versionTable.insert(pair<string, vector<vector<Attribute>>>(tableName, result));

		return result;
	}
	else if (tableName == "Columns")
	{
		vector<vector<Attribute>> result;
		vector<Attribute> attrs1;

		Attribute attr1;
		attr1.name = "table-id";
		attr1.type = TypeInt;
		attr1.length = (AttrLength)4;
		attrs1.push_back(attr1);

		attr1.name = "column-name";
		attr1.type = TypeVarChar;
		attr1.length = (AttrLength)50;
		attrs1.push_back(attr1);

		attr1.name = "column-type";
		attr1.type = TypeInt;
		attr1.length = (AttrLength)4;
		attrs1.push_back(attr1);

		attr1.name = "column-length";
		attr1.type = TypeInt;
		attr1.length = (AttrLength)4;
		attrs1.push_back(attr1);

		attr1.name = "column-position";
		attr1.type = TypeInt;
		attr1.length = (AttrLength)4;
		attrs1.push_back(attr1);

		attr1.name = "startVersion";
		attr1.type = TypeInt;
		attr1.length = (AttrLength)4;
		attrs1.push_back(attr1);

		attr1.name = "endVersion";
		attr1.type = TypeInt;
		attr1.length = (AttrLength)4;
		attrs1.push_back(attr1);

		result.push_back(attrs1);

		map_versionTable.insert(pair<string, vector<vector<Attribute>>>(tableName, result));

		return result;
	}
	else
	{
		fm_table->openFile(tableName, fh_table);
		unsigned currentVersion = fh_table.currentVersion;
		vector<vector<Attribute>> result(currentVersion + 1);
		fm_table->closeFile(fh_table);

		string attr = "table-id";
		vector<string> attributes;
		attributes.push_back(attr);
		RM_ScanIterator rmsi;
		int size = tableName.size();
		void *data1 = malloc(size + 4);
		memcpy((char *)data1, &size, 4);
		memcpy((char *)data1 + 4, tableName.c_str(), size);
		scan("Tables", "table-name", EQ_OP, data1, attributes, rmsi);
		free(data1);
		int nullAttributesIndicatorActualSize1 = ceil((double)4 / CHAR_BIT);
		RID rid;
		void *returnedData = malloc(200);
		rmsi.getNextTuple(rid, returnedData);
		int tableID = *(int *)((char *)returnedData + nullAttributesIndicatorActualSize1);
		fm_table->closeFile(fh_table);
		rmsi.close();

		vector<string> attribute1;
		attribute1.push_back("column-name");
		attribute1.push_back("column-type");
		attribute1.push_back("column-length");
		attribute1.push_back("startVersion");
		attribute1.push_back("endVersion");
		int nullAttributesIndicatorActualSize = ceil((double)5 / CHAR_BIT);
		void *data = malloc(4);
		memcpy((char *)data, &tableID, 4);
		scan("Columns", "table-id", EQ_OP, data, attribute1, rmsi);
		free(data);
		returnedData = realloc(returnedData, 200);
		while (rmsi.getNextTuple(rid, returnedData) != RM_EOF)
		{
			Attribute attr1;
			int offset = 0;
			int nameSize = *(int *)((char *)returnedData + offset + nullAttributesIndicatorActualSize);
			offset += 4;
			char* fieldName = (char *)malloc(nameSize + 1);
			memcpy(fieldName, (char *)returnedData + nullAttributesIndicatorActualSize + offset, nameSize);
			fieldName[nameSize] = '\0';
			offset += nameSize;
			string stringfieldName = fieldName;
			attr1.name = stringfieldName;
			int fieldType = *(int *)((char *)returnedData + offset + nullAttributesIndicatorActualSize);
			offset += 4;
			attr1.type = (AttrType)fieldType;
			int maxLength = *(int *)((char *)returnedData + offset + nullAttributesIndicatorActualSize);
			attr1.length = (AttrLength)maxLength;
			offset += sizeof(int);
			int startVersion = *(int *)((char *)returnedData + offset + nullAttributesIndicatorActualSize);
			offset += sizeof(int);
			int endVersion = *(int *)((char *)returnedData + offset + nullAttributesIndicatorActualSize);

			for (int i = startVersion; i <= endVersion; i++)
				result[i].push_back(attr1);

			free(fieldName);
		}
		free(returnedData);
		fm_table->closeFile(fh_table);
		rmsi.close();

		map_versionTable.insert(pair<string, vector<vector<Attribute>>>(tableName, result));

		return result;
	}
}

vector<Attribute>* RelationManager::getVersionTable(const int version)
{
	return &this->versionTable[version];
}

RC RelationManager::createIndex(const string &tableName, const string &attributeName)
{
	return -1;
}

RC RelationManager::destroyIndex(const string &tableName, const string &attributeName)
{
	return -1;
}

RC RelationManager::indexScan(const string &tableName,
	const string &attributeName,
	const void *lowKey,
	const void *highKey,
	bool lowKeyInclusive,
	bool highKeyInclusive,
	RM_IndexScanIterator &rm_IndexScanIterator)
{
	return -1;
}