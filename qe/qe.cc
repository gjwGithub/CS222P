#include "qe.h"

int getAttrIndex(const vector<Attribute> &attrs, const string &attr)
{
	for (size_t i = 0; i < attrs.size(); i++)
	{
		if (attrs[i].name == attr)
			return i;
	}
	return -1;
}

Value getAttributeValue(const void* data, const int index, const vector<Attribute> &attrs)
{
	int offset = ceil((double)attrs.size() / CHAR_BIT);
	for (int i = 0; i < index; i++)
	{
		unsigned char nullFields = ((unsigned char*)data)[i / CHAR_BIT];
		bool isNULL = ((unsigned char*)data)[i / CHAR_BIT] & (1 << (CHAR_BIT - 1 - i % CHAR_BIT));
		if (!isNULL)
		{
			if (attrs[i].type == AttrType::TypeInt)
				offset += sizeof(int);
			else if (attrs[i].type == AttrType::TypeReal)
				offset += sizeof(float);
			else if (attrs[i].type == AttrType::TypeVarChar)
				offset += sizeof(int) + *(int*)((char*)data + offset);
		}
	}
	Value result;
	result.data = (char*)data + offset;
	result.type = attrs[index].type;
	return result;
}

bool compareAttributes(void* v1, void* v2, CompOp op, AttrType type)
{
	if (type == AttrType::TypeInt)
	{
		int a = *(int*)v1;
		int b = *(int*)v2;
		if (op == CompOp::EQ_OP)
			return a == b;
		else if (op == CompOp::GE_OP)
			return a >= b;
		else if (op == CompOp::GT_OP)
			return a > b;
		else if (op == CompOp::LE_OP)
			return a <= b;
		else if (op == CompOp::LT_OP)
			return a < b;
		else if (op == CompOp::NE_OP)
			return a != b;
		else
			return true;
	}
	else if (type == AttrType::TypeReal)
	{
		float a = *(float*)v1;
		float b = *(float*)v2;
		if (op == CompOp::EQ_OP)
			return a == b;
		else if (op == CompOp::GE_OP)
			return a >= b;
		else if (op == CompOp::GT_OP)
			return a > b;
		else if (op == CompOp::LE_OP)
			return a <= b;
		else if (op == CompOp::LT_OP)
			return a < b;
		else if (op == CompOp::NE_OP)
			return a != b;
		else
			return true;
	}
	else if (type == AttrType::TypeVarChar)
	{
		int strLength1 = *(int*)v1;
		int strLength2 = *(int*)v2;
		string a((char*)v1 + sizeof(int), strLength1);
		string b((char*)v2 + sizeof(int), strLength2);
		if (op == CompOp::EQ_OP)
			return a == b;
		else if (op == CompOp::GE_OP)
			return a >= b;
		else if (op == CompOp::GT_OP)
			return a > b;
		else if (op == CompOp::LE_OP)
			return a <= b;
		else if (op == CompOp::LT_OP)
			return a < b;
		else if (op == CompOp::NE_OP)
			return a != b;
		else
			return true;
	}
}

bool Value::operator<(const Value& rhs) const
{
	return (this->type == rhs.type) && compareAttributes(this->data, rhs.data, LT_OP, this->type);
}

Filter::Filter(Iterator* input, const Condition &condition)
{
	this->input = input;
	this->condition = condition;
	this->input->getAttributes(this->attrs);
	this->leftIndex = getAttrIndex(this->attrs, this->condition.lhsAttr);
	if (this->condition.bRhsIsAttr)
		this->rightIndex = getAttrIndex(this->attrs, this->condition.rhsAttr);
	else
		this->rightIndex = 0;
	this->end = false;
}

RC Filter::getNextTuple(void *data)
{
	if (!this->end)
	{
		while (this->input->getNextTuple(data) != QE_EOF)
		{
			if (this->condition.op == NO_OP)
				return 0;
			Value leftValue = getAttributeValue(data, this->leftIndex, this->attrs);
			Value rightValue;
			if (this->condition.bRhsIsAttr)
				rightValue = getAttributeValue(data, this->rightIndex, this->attrs);
			else
				rightValue = this->condition.rhsValue;
			if (compareAttributes(leftValue.data, rightValue.data, this->condition.op, leftValue.type))
				return 0;
		}
		this->end = true;
	}
	return QE_EOF;
}

void Filter::getAttributes(vector<Attribute> &attrs) const
{
	attrs = this->attrs;
}
// ... the rest of your implementations go here

Project::Project(Iterator *input,                    // Iterator of input R
	const vector<string> &attrNames)    // vector containing attribute names
{
	this->input = input;
	vector<Attribute> allAttrs;
	this->input->getAttributes(allAttrs);
	this->totalAttrsCount = allAttrs.size();
	for (auto &i : attrNames)
	{
		for (size_t j = 0; j < allAttrs.size(); j++)
		{
			if (allAttrs[j].name == i)
			{
				this->attrIndexes.push_back(j);
				this->attrs.push_back(allAttrs[j]);
			}
		}
	}
	this->end = false;
}

RC Project::getNextTuple(void *data)
{
	this->end = input->getNextTuple(this->buffer);
	if (!this->end)
	{
		int dataNullIndicatorSize = ceil((double)this->attrs.size() / CHAR_BIT);
		int dataOffset = dataNullIndicatorSize;
		int bufferNullIndicatorSize = ceil((double)this->totalAttrsCount / CHAR_BIT);
		int bufferOffset = bufferNullIndicatorSize;
		int count = 0;
		memset(data, 0, dataNullIndicatorSize);
		for (size_t i = 0; i < this->totalAttrsCount; i++)
		{
			unsigned char nullFields = ((unsigned char*)data)[count / CHAR_BIT];
			bool isNULL = ((unsigned char*)this->buffer)[count / CHAR_BIT] & (1 << (CHAR_BIT - 1 - count % CHAR_BIT));
			if ((int)i != this->attrIndexes[count])
			{
				if (!isNULL)
				{
					if (attrs[count].type == AttrType::TypeInt)
						bufferOffset += sizeof(int);
					else if (attrs[count].type == AttrType::TypeReal)
						bufferOffset += sizeof(float);
					else if (attrs[count].type == AttrType::TypeVarChar)
					{
						int strLength = *(int*)(this->buffer + bufferOffset);
						bufferOffset += sizeof(int) + strLength;
					}
				}
				continue;
			}
			else
			{
				if (isNULL)
					nullFields += 1 << (CHAR_BIT - 1 - count % CHAR_BIT);
				else
				{
					nullFields += 0 << (CHAR_BIT - 1 - count % CHAR_BIT);
					if (attrs[count].type == AttrType::TypeInt)
					{
						memcpy((char*)data + dataOffset, this->buffer + bufferOffset, sizeof(int));
						dataOffset += sizeof(int);
						bufferOffset += sizeof(int);
					}
					else if (attrs[count].type == AttrType::TypeReal)
					{
						memcpy((char*)data + dataOffset, this->buffer + bufferOffset, sizeof(float));
						dataOffset += sizeof(float);
						bufferOffset += sizeof(float);
					}
					else if (attrs[count].type == AttrType::TypeVarChar)
					{
						int strLength = *(int*)(this->buffer + bufferOffset);
						memcpy((char*)data + dataOffset, this->buffer + bufferOffset, sizeof(int) + strLength);
						dataOffset += sizeof(int) + strLength;
						bufferOffset += sizeof(int) + strLength;
					}
				}
				((unsigned char*)data)[count / CHAR_BIT] = nullFields;
				++count;
			}
		}
		return 0;
	}
	return QE_EOF;
}

void Project::getAttributes(vector<Attribute> &attrs) const
{
	attrs = this->attrs;
}

BNLJoin::BNLJoin(Iterator *leftIn, TableScan *rightIn, const Condition &condition, const unsigned numPages) {
	this->leftIn = leftIn;
	this->rightIn = rightIn;
	this->condition = condition;
	this->numPages = numPages;
	this->leftIn->getAttributes(this->attrs_out);
	updateVectorOuter();
	this->rightIn->getAttributes(this->attrs_in);
	updateVectorInner();
}
RC BNLJoin::updateVectorOuter() {
	sum_buffer = 0;
	outers.clear();
	while (sum_buffer<numPages*PAGE_SIZE) {
		void *data = malloc(PAGE_SIZE);
		if (this->leftIn->getNextTuple(data) != QE_EOF) {
			int sig_length;
			getByteLength(this->attrs_out, data, sig_length);
			sum_buffer += sig_length;
			Tuple tuple1(data, sig_length);
			outers.push_back(tuple1);
			free(data);
		}
		else {
			free(data);
			return -1;
		}
	}
	return 0;

}
RC BNLJoin::updateVectorInner() {
	sum_buffer = 0;
	inners.clear();
	while (sum_buffer<PAGE_SIZE) {
		void *data = malloc(PAGE_SIZE);
		if (this->rightIn->getNextTuple(data) != QE_EOF) {
			int sig_length;
			getByteLength(this->attrs_in, data, sig_length);
			sum_buffer += sig_length;
			Tuple tuple1(data, sig_length);
			inners.push_back(tuple1);
			free(data);
		}
		else {
			free(data);
			return -1;
		}
	}
	return 0;
}

Tuple::Tuple(void *data, int length) {
	this->length = length;
	this->data = malloc(length);
	memcpy((char *)this->data, (char *)data, length);
}

RC BNLJoin::getByteLength(vector<Attribute> attrs, void *data, int &size) {
	int offset = 0; // offset is the length of record
	int size1 = attrs.size();
	int byte_count = ceil((double)size1 / 8);

	//calcuate the length of record;
	char* bites = (char*)malloc(byte_count);//the number of fields
	memcpy(bites, (char *)data + offset, byte_count);
	offset = offset + byte_count;
	for (int i = 0; i<size1; i++) {
		if (!(bites[i / 8] & (1 << (7 - i % 8)))) {
			if (attrs[i].type == TypeVarChar) {
				int string_size;
				memcpy(&string_size, (char *)data + offset, 4);
				offset = offset + string_size + 4;
			}
			else {
				offset = offset + 4;
			}
		}
	}
	size = offset;
}
RC BNLJoin::find_r_value(int &attrtype, int &value_int, float &value_float, string &value_string, int attr_index) {
	int offset = 0; // offset is the length of record
	int size = attrs_out.size();
	int byte_count = ceil((double)size / 8);
	//calcuate the length of record;
	char* bites = (char*)malloc(byte_count);//the number of fields
	memcpy(bites, (char *)outers[outer_index].data + offset, byte_count);
	offset = offset + byte_count;
	for (int i = 0; i<size; i++) {
		if (!(bites[i / 8] & (1 << (7 - i % 8)))) {
			if (attrs_out[i].type == TypeVarChar) {
				int string_size;
				memcpy(&string_size, (char *)outers[outer_index].data + offset, 4);
				offset = offset + string_size + 4;
				if (i == attr_index) {
					attrtype = 3;
					char *value_char = (char *)malloc(string_size + 1);
					memcpy(value_char, (char *)outers[outer_index].data + offset - string_size, string_size);
					value_char[string_size] = '\0';
					value_string = value_char;
					break;
				}
			}
			else if (attrs_out[i].type == TypeInt) {
				if (i == attr_index) {
					memcpy(&value_int, (char *)outers[outer_index].data + offset, sizeof(int));
					attrtype = 1;
					break;
				}
				offset = offset + 4;
			}
			else {
				if (i == attr_index) {
					memcpy(&value_float, (char *)outers[outer_index].data + offset, sizeof(float));
					attrtype = 2;
					break;
				}
				offset = offset + 4;
			}
		}

	}
}
RC BNLJoin::find_s_value(int &attrtype, int &value_int, float &value_float, string &value_string, int inner_index, int attr_index) {
	int offset = 0; // offset is the length of record
	int size = attrs_in.size();
	int byte_count = ceil((double)size / 8);
	//calcuate the length of record;
	char* bites = (char*)malloc(byte_count);//the number of fields
	memcpy(bites, (char *)inners[inner_index].data + offset, byte_count);
	offset = offset + byte_count;
	for (int i = 0; i<size; i++) {
		if (!(bites[i / 8] & (1 << (7 - i % 8)))) {
			if (attrs_in[i].type == TypeVarChar) {
				int string_size;
				memcpy(&string_size, (char *)inners[inner_index].data + offset, 4);
				offset = offset + string_size + 4;
				if (i == attr_index) {
					attrtype = 3;
					char *value_char = (char *)malloc(string_size + 1);
					memcpy(value_char, (char *)inners[inner_index].data + offset - string_size, string_size);
					value_char[string_size] = '\0';
					value_string = value_char;
					break;
				}
			}
			else if (attrs_in[i].type == TypeInt) {
				if (i == attr_index) {
					memcpy(&value_int, (char *)inners[inner_index].data + offset, sizeof(int));
					attrtype = 1;
					break;
				}
				offset = offset + 4;
			}
			else {
				if (i == attr_index) {
					memcpy(&value_float, (char *)inners[inner_index].data + offset, sizeof(float));
					attrtype = 2;
					break;
				}
				offset = offset + 4;
			}
		}

	}
}
RC BNLJoin::getNextTuple(void *data) {
	while (true) {
		while (outer_index<outers.size()) {
			int attr_index;
			for (int i = 0; i<attrs_out.size(); i++) {
				if (attrs_out[i].name == condition.lhsAttr) {
					attr_index = i;
					break;
				}
			}
			//find the R value
			int attrtype = 0;
			int value_int;
			float value_float;
			string value_string;
			find_r_value(attrtype, value_int, value_float, value_string, attr_index);
			//find s value
			int attr_right_index;
			for (int i = 0; i<attrs_in.size(); i++) {
				if (attrs_in[i].name == condition.rhsAttr) {
					attr_right_index = i;
					break;
				}
			}
			for (int i = inner_index; i<inners.size(); i++) {
				int attrtype1 = 0;
				int value_int1;
				float value_float1;
				string value_string1;
				find_s_value(attrtype1, value_int1, value_float1, value_string1, i, attr_right_index);
				if (attrtype == 1) {
					if (value_int1 == value_int) {
						getJoin(data, outers[outer_index].data, outers[outer_index].length, inners[i].data, inners[i].length);
						inner_index = i + 1;
						return 0;
					}
				}
				else if (attrtype == 2) {
					if (value_float == value_float1) {
						getJoin(data, outers[outer_index].data, outers[outer_index].length, inners[i].data, inners[i].length);
						inner_index = i + 1;
						return 0;
					}
				}
				else if (attrtype == 3) {
					if (value_string1 == value_string) {
						getJoin(data, outers[outer_index].data, outers[outer_index].length, inners[i].data, inners[i].length);
						inner_index = i + 1;
						return 0;
					}
				}
			}
			outer_index++;
		}
		if (outer_index >= outers.size()) {
			RC rel = updateVectorInner();
			inner_index = 0;
			outer_index = 0;
			if (inners.size() == 0) {
				RC rel1 = updateVectorOuter();
				inner_index = 0;
				outer_index = 0;
				rightIn->setIterator();
				RC rel2 = updateVectorInner();
				if (outers.size() == 0)
					return QE_EOF;
			}
		}
	}
}
RC BNLJoin::getJoin(void *data, void *r_data, int outer_length, void *s_data, int inner_length) {
	int offset = 0; // offset is the length of record
	int size = attrs_out.size();
	int byte_count = ceil((double)size / 8);
	char* bites = (char*)malloc(byte_count);//the number of fields
	memcpy(bites, (char *)r_data + offset, byte_count);

	int offset1 = 0;
	int size1 = attrs_in.size();
	int byte_count1 = ceil((double)size1 / 8);
	char* bites1 = (char*)malloc(byte_count1);//the number of fields
	memcpy(bites1, (char *)s_data + offset1, byte_count1);


	int join_size = size + size1;
	int join_byte_count = ceil((double)join_size / 8);
	//calcuate the length of record;
	int join_offset = join_byte_count;

	memcpy((char *)data, bites, byte_count);
	for (int i = size; i<join_size; i++) {
		if (!(bites1[(i - size) / 8] & (1 << (7 - (i - size) % 8))));
		else {
			((char *)data)[i / 8] = ((char *)data)[i / 8] | (1 << (7 - (i - size) % 8));
		}
	}
	memcpy((char *)data + join_offset, (char *)r_data + byte_count, outer_length - byte_count);
	memcpy((char *)data + join_offset + outer_length - byte_count, (char *)s_data + byte_count1, inner_length - byte_count1);
}
void BNLJoin::getAttributes(vector<Attribute> &attrs) const {
	for (int i = 0; i<attrs_out.size(); i++) {
		attrs.push_back(attrs_out[i]);
	}
	for (int i = 0; i<attrs_in.size(); i++) {
		attrs.push_back(attrs_in[i]);
	}
}

INLJoin::INLJoin(Iterator *leftIn,           // Iterator of input R
	IndexScan *rightIn,          // IndexScan Iterator of input S
	const Condition &condition)
{
	this->leftIn = leftIn;
	this->rightIn = rightIn;
	this->condition = condition;

	this->leftIn->getAttributes(this->leftAttrs);
	this->leftIndex = getAttrIndex(this->leftAttrs, this->condition.lhsAttr);

	this->rightIn->getAttributes(this->rightAttrs);
	this->rightIndex = getAttrIndex(this->rightAttrs, this->condition.rhsAttr);

	if (this->leftIn->getNextTuple(this->leftBuffer) == QE_EOF)
		this->end = true;
	else
	{
		Value leftValue = getAttributeValue(this->leftBuffer, this->leftIndex, this->leftAttrs);
		this->rightIn->setIterator(leftValue.data, leftValue.data, true, true);
	}
}

RC INLJoin::getNextTuple(void *data)
{
	if (readFromRight(this->rightIn, data) != QE_EOF)
		return 0;
	while (leftIn->getNextTuple(leftBuffer) != QE_EOF)
	{
		Value leftValue = getAttributeValue(leftBuffer, leftIndex, leftAttrs);
		this->rightIn->setIterator(leftValue.data, leftValue.data, true, true);
		if (readFromRight(this->rightIn, data) != QE_EOF)
			return 0;
	}
	return IX_EOF;
}

RC INLJoin::readFromRight(IndexScan* rightScan, void * data)
{
	if (rightScan->getNextTuple(this->rightBuffer) != QE_EOF)
		return outputJoinResult(data);
	return QE_EOF;
}

RC INLJoin::outputJoinResult(void *data)
{
	int nullIndicatorSize = ceil((double)(this->leftAttrs.size() + this->rightAttrs.size()) / CHAR_BIT);
	memset(data, 0, nullIndicatorSize);
	int offset = nullIndicatorSize;
	//Copy left attributes
	int leftOffset = ceil((double)this->leftAttrs.size() / CHAR_BIT);
	for (size_t i = 0; i < this->leftAttrs.size(); i++)
	{
		unsigned char nullFields = ((unsigned char*)data)[i / CHAR_BIT];
		bool isNULL = this->leftBuffer[i / CHAR_BIT] & (1 << (CHAR_BIT - 1 - i % CHAR_BIT));
		if (isNULL)
			nullFields += 1 << (CHAR_BIT - 1 - i % CHAR_BIT);
		else
		{
			nullFields += 0 << (CHAR_BIT - 1 - i % CHAR_BIT);
			if (leftAttrs[i].type == AttrType::TypeInt)
			{
				memcpy((char*)data + offset, leftBuffer + leftOffset, sizeof(int));
				offset += sizeof(int);
				leftOffset += sizeof(int);
			}
			else if (leftAttrs[i].type == AttrType::TypeReal)
			{
				memcpy((char*)data + offset, leftBuffer + leftOffset, sizeof(float));
				offset += sizeof(float);
				leftOffset += sizeof(float);
			}
			else if (leftAttrs[i].type == AttrType::TypeVarChar)
			{
				int strLength = *(int*)(leftBuffer + leftOffset);
				memcpy((char*)data + offset, leftBuffer + leftOffset, sizeof(int) + strLength);
				offset += sizeof(int) + strLength;
				leftOffset += sizeof(int) + strLength;
			}
		}
		((unsigned char*)data)[i / CHAR_BIT] = nullFields;
	}
	//Copy right attributes
	int rightOffset = ceil((double)this->rightAttrs.size() / CHAR_BIT);
	for (size_t i = 0; i < this->rightAttrs.size(); i++)
	{
		unsigned char nullFields = ((unsigned char*)data)[(i + this->leftAttrs.size()) / CHAR_BIT];
		bool isNULL = this->rightBuffer[i / CHAR_BIT] & (1 << (CHAR_BIT - 1 - i % CHAR_BIT));
		if (isNULL)
			nullFields += 1 << (CHAR_BIT - 1 - (i + this->leftAttrs.size()) % CHAR_BIT);
		else
		{
			nullFields += 0 << (CHAR_BIT - 1 - (i + this->leftAttrs.size()) % CHAR_BIT);
			if (rightAttrs[i].type == AttrType::TypeInt)
			{
				memcpy((char*)data + offset, rightBuffer + rightOffset, sizeof(int));
				offset += sizeof(int);
				rightOffset += sizeof(int);
			}
			else if (rightAttrs[i].type == AttrType::TypeReal)
			{
				memcpy((char*)data + offset, rightBuffer + rightOffset, sizeof(float));
				offset += sizeof(float);
				rightOffset += sizeof(float);
			}
			else if (rightAttrs[i].type == AttrType::TypeVarChar)
			{
				int strLength = *(int*)(rightBuffer + rightOffset);
				memcpy((char*)data + offset, rightBuffer + rightOffset, sizeof(int) + strLength);
				offset += sizeof(int) + strLength;
				rightOffset += sizeof(int) + strLength;
			}
		}
		((unsigned char*)data)[(i + this->leftAttrs.size()) / CHAR_BIT] = nullFields;
	}
	return 0;
}

void INLJoin::getAttributes(vector<Attribute>& attrs) const
{
	attrs = this->leftAttrs;
	attrs.insert(attrs.end(), this->rightAttrs.begin(), this->rightAttrs.end());
}

Aggregate::Aggregate(Iterator *input,          // Iterator of input R
	Attribute aggAttr,        // The attribute over which we are computing an aggregate
	AggregateOp op            // Aggregate operation
)
{
	this->input = input;
	this->aggAttr = aggAttr;
	this->op = op;
	this->input->getAttributes(this->attrs);
	this->attrIndex = getAttrIndex(this->attrs, this->aggAttr.name);
	this->end = false;
	this->isGroupby = false;
}

RC Aggregate::getNextTuple(void *data)
{
	if (this->end)
		return QE_EOF;
	((char*)data)[0] = 0;
	if (!this->isGroupby)
	{
		switch (this->op)
		{
		case AggregateOp::AVG:
		{
			char* temp = (char*)malloc(PAGE_SIZE);
			double sum = 0;
			int count = 0;
			while (this->input->getNextTuple(temp) != QE_EOF)
			{
				Value value = getAttributeValue(temp, this->attrIndex, this->attrs);
				if (value.type == AttrType::TypeInt)
					sum += *(int*)value.data;
				else if (value.type == AttrType::TypeReal)
					sum += *(float*)value.data;
				++count;
			}
			if (count == 0)
			{
#ifdef DEBUG
				cerr << "The number of tuples is 0 when calculating AVG" << endl;
#endif
				return -1;
			}
			float result = sum / count;
			memcpy((char*)data + 1, &result, sizeof(float));
			free(temp);
		}
		break;
		case AggregateOp::COUNT:
		{
			char* temp = (char*)malloc(PAGE_SIZE);
			float count = 0;
			while (this->input->getNextTuple(temp) != QE_EOF)
				++count;
			memcpy((char*)data + 1, &count, sizeof(float));
			free(temp);
		}
		break;
		case AggregateOp::MAX:
		{
			char* temp = (char*)malloc(PAGE_SIZE);
			float max = numeric_limits<float>::min();
			while (this->input->getNextTuple(temp) != QE_EOF)
			{
				Value value = getAttributeValue(temp, this->attrIndex, this->attrs);
				if (value.type == AttrType::TypeInt)
					max = (float)*(int*)value.data > max ? (float)*(int*)value.data : max;
				else if (value.type == AttrType::TypeReal)
					max = *(float*)value.data > max ? *(float*)value.data : max;
			}
			memcpy((char*)data + 1, &max, sizeof(float));
			free(temp);
		}
		break;
		case AggregateOp::MIN:
		{
			char* temp = (char*)malloc(PAGE_SIZE);
			float min = numeric_limits<float>::max();
			while (this->input->getNextTuple(temp) != QE_EOF)
			{
				Value value = getAttributeValue(temp, this->attrIndex, this->attrs);
				if (value.type == AttrType::TypeInt)
					min = (float)*(int*)value.data < min ? (float)*(int*)value.data : min;
				else if (value.type == AttrType::TypeReal)
					min = *(float*)value.data < min ? *(float*)value.data : min;
			}
			memcpy((char*)data + 1, &min, sizeof(float));
			free(temp);
		}
		break;
		case AggregateOp::SUM:
		{
			char* temp = (char*)malloc(PAGE_SIZE);
			float sum = 0;
			while (this->input->getNextTuple(temp) != QE_EOF)
			{
				Value value = getAttributeValue(temp, this->attrIndex, this->attrs);
				if (value.type == AttrType::TypeInt)
					sum += (float)*(int*)value.data;
				else if (value.type == AttrType::TypeReal)
					sum += *(float*)value.data;
			}
			memcpy((char*)data + 1, &sum, sizeof(float));
			free(temp);
		}
		break;
		default:
			break;
		}
		this->end = true;
	}
	else
	{
		if (this->groupResultIter != this->groupResult.end())
		{
			int keyLength = 0;
			if (this->groupResultIter->first.type == AttrType::TypeInt)
				keyLength = sizeof(int);
			else if (this->groupResultIter->first.type == AttrType::TypeReal)
				keyLength = sizeof(float);
			else if (this->groupResultIter->first.type == AttrType::TypeVarChar)
				keyLength = sizeof(int) + *(int*)this->groupResultIter->first.data;
			memcpy((char*)data + 1, this->groupResultIter->first.data, keyLength);
			switch (this->op)
			{
			case AggregateOp::AVG:
				memcpy((char*)data + 1 + keyLength, &this->groupResultIter->second.avg, sizeof(float));
				break;
			case AggregateOp::COUNT:
				memcpy((char*)data + 1 + keyLength, &this->groupResultIter->second.count, sizeof(float));
				break;
			case AggregateOp::MAX:
				memcpy((char*)data + 1 + keyLength, &this->groupResultIter->second.max, sizeof(float));
				break;
			case AggregateOp::MIN:
				memcpy((char*)data + 1 + keyLength, &this->groupResultIter->second.min, sizeof(float));
				break;
			case AggregateOp::SUM:
				memcpy((char*)data + 1 + keyLength, &this->groupResultIter->second.sum, sizeof(float));
				break;
			default:
				break;
			}
			++this->groupResultIter;
		}
		else
		{
			this->end = true;
			return QE_EOF;
		}
	}
	return 0;
}

void Aggregate::getAttributes(vector<Attribute> &attrs) const
{
	Attribute attr = this->aggAttr;
	const string AggregateOpNames[] = { "MIN", "MAX", "COUNT", "SUM", "AVG" };
	attr.name = AggregateOpNames[op] + "(" + aggAttr.name + ")";
	attrs.push_back(attr);
}

Aggregate::Aggregate(Iterator *input,             // Iterator of input R
	Attribute aggAttr,           // The attribute over which we are computing an aggregate
	Attribute groupAttr,         // The attribute over which we are grouping the tuples
	AggregateOp op              // Aggregate operation
)
{
	this->input = input;
	this->aggAttr = aggAttr;
	this->op = op;
	this->input->getAttributes(this->attrs);
	this->attrIndex = getAttrIndex(this->attrs, this->aggAttr.name);
	this->end = false;
	this->isGroupby = true;

	int groupAttrIndex = getAttrIndex(this->attrs, groupAttr.name);
	char data[PAGE_SIZE];
	while (input->getNextTuple(data) != QE_EOF)
	{
		Value groupValue = getAttributeValue(data, groupAttrIndex, this->attrs);
		Value key;
		int keyLength = 0;
		if (groupValue.type == AttrType::TypeInt)
			keyLength = sizeof(int);
		else if (groupValue.type == AttrType::TypeReal)
			keyLength = sizeof(float);
		else if (groupValue.type == AttrType::TypeVarChar)
			keyLength = sizeof(int) + *(int*)groupValue.data;
		key.type = groupValue.type;

		auto search = this->groupResult.find(groupValue);
		AggregateResult results;
		if (search != this->groupResult.end())
		{
			results = search->second;
			key.data = search->first.data;
		}
		else
		{
			key.data = malloc(keyLength);
			memcpy(key.data, groupValue.data, keyLength);
		}
		Value value = getAttributeValue(data, this->attrIndex, this->attrs);
		if (value.type == AttrType::TypeInt)
		{
			int v = *(int*)value.data;
			results.sum += v;
			++results.count;
			results.avg = results.sum / results.count;
			results.min = v < results.min ? v : results.min;
			results.max = v > results.max ? v : results.max;
		}
		else if (value.type == AttrType::TypeReal)
		{
			float v = *(float*)value.data;
			results.sum += v;
			++results.count;
			results.avg = results.sum / results.count;
			results.min = v < results.min ? v : results.min;
			results.max = v > results.max ? v : results.max;
		}

		if (search != this->groupResult.end())
			this->groupResult[key] = results;
		else
			this->groupResult.insert(make_pair(key, results));
	}

	if (this->groupResult.size() == 0)
		this->end = true;
	else
		this->groupResultIter = this->groupResult.begin();
}

Aggregate::~Aggregate()
{
	for (auto &i : this->groupResult)
		free(i.first.data);
}
int GHJoin::uniq_id=0;
GHJoin::GHJoin(Iterator *leftIn,Iterator *rightIn,const Condition &condition,const unsigned numPartitions){
	this->leftIn = leftIn;
	this->rightIn = rightIn;
	this->condition = condition;
	this->numPartitions = numPartitions;
	this->leftIn->getAttributes(this->attrs_out);
	this->rightIn->getAttributes(this->attrs_in);
	string r_attr_name=attrs_out[0].name;
	for(int i=0;i<r_attr_name.size();i++){
		if(r_attr_name[i]=='.'){
			r_tableName=r_attr_name.substr(0,i);
		}
	}
	string s_attr_name=attrs_in[0].name;
	for(int i=0;i<s_attr_name.size();i++){
		if(s_attr_name[i]=='.'){
			s_tableName=s_attr_name.substr(0,i);
		}
	}
	FILE* file;
	string aaa="left_join_"+to_string(uniq_id)+"_"+r_tableName+"0";
	file = fopen(aaa.c_str(), "rb+");
	if (file == NULL) {
	}
	else {
		uniq_id++;
		fclose(file);
	}
	name_id=uniq_id;
	for(int i=0;i<numPartitions;i++){
		string left_table="left_join_"+to_string(name_id)+"_"+r_tableName + to_string(i);
		RelationManager::instance()->createTable(left_table,attrs_out);
		string right_table="right_join_" +to_string(name_id)+"_"+ s_tableName + to_string(i);
		RelationManager::instance()->createTable(right_table,attrs_in);
	}
	fillLeftPartitions();
	fillRightPartitions();
	map1.clear();
	map2.clear();
	map3.clear();
	for(int i=0;i<attrs_in.size();i++){
		string_vector_in.push_back(attrs_in[i].name);
	}
	for(int i=0;i<attrs_out.size();i++){
		string_vector_out.push_back(attrs_out[i].name);
	}
	map_rPartitions(cur_partition);
	RelationManager::instance()->scan("right_join_"+to_string(name_id)+"_"+s_tableName+"0","",NO_OP,NULL,string_vector_in,rm_ite);
	data_s=malloc(PAGE_SIZE);
	rm_ite.getNextTuple(rid,data_s);
}
void GHJoin::fillLeftPartitions(){
	int attr_index;
	for (int i = 0; i<attrs_out.size(); i++) {
		if (attrs_out[i].name == condition.lhsAttr) {
			attr_index = i;
			break;
		}
	}
	//cout<<"index:"<<attr_index<<endl;
	void *data = malloc(PAGE_SIZE);
	while(this->leftIn->getNextTuple(data) != QE_EOF){
		int attrtype = 0;
		int value_int;
		float value_float;
		string value_string;
		find_r_value(attrtype, value_int, value_float, value_string, attr_index, data);
		if(attrtype==1){
			int fileindex = value_int % numPartitions;
			RelationManager::instance()->insertTuple("left_join_"+to_string(name_id)+"_"+r_tableName+to_string(fileindex),data,rid);
		}else if(attrtype==2){
			int fileindex = (int)value_float % numPartitions;
			RelationManager::instance()->insertTuple("left_join_"+to_string(name_id)+"_"+r_tableName+to_string(fileindex),data,rid);
		}else{
			int fileindex=(value_string[0]-'!') % numPartitions;
			RelationManager::instance()->insertTuple("left_join_"+to_string(name_id)+"_"+r_tableName+to_string(fileindex),data,rid);
		}
		free(data);
		data=malloc(PAGE_SIZE);
	}
	free(data);
}
RC GHJoin::find_r_value(int &attrtype, int &value_int, float &value_float, string &value_string, int attr_index,void* data) {
	int offset = 0; // offset is the length of record
	int size = attrs_out.size();
	int byte_count = ceil((double)size / 8);
	//calcuate the length of record;
	char* bites = (char*)malloc(byte_count);//the number of fields
	memcpy(bites, (char *)data + offset, byte_count);
	offset = offset + byte_count;
	for (int i = 0; i<size; i++) {
		if (!(bites[i / 8] & (1 << (7 - i % 8)))) {
			if (attrs_out[i].type == TypeVarChar) {
				int string_size;
				memcpy(&string_size, (char *)data + offset, 4);
				offset = offset + string_size + 4;
				if (i == attr_index) {
					attrtype = 3;
					char *value_char = (char *)malloc(string_size + 1);
					memcpy(value_char, (char *)data + offset - string_size, string_size);
					value_char[string_size] = '\0';
					value_string = value_char;
					free(value_char);
					break;
				}
			}
			else if (attrs_out[i].type == TypeInt) {
				if (i == attr_index) {
					memcpy(&value_int, (char *)data + offset, sizeof(int));
					attrtype = 1;
					break;
				}
				offset = offset + 4;
			}
			else {
				if (i == attr_index) {
					memcpy(&value_float, (char *)data + offset, sizeof(float));
					attrtype = 2;
					break;
				}
				offset = offset + 4;
			}
		}

	}
	free(bites);
}
void GHJoin::fillRightPartitions(){
	int attr_right_index;
	for (int i = 0; i<attrs_in.size(); i++) {
		if (attrs_in[i].name == condition.rhsAttr) {
			attr_right_index = i;
			break;
		}
	}
	//find the R value
	void *data = malloc(PAGE_SIZE);
	while(this->rightIn->getNextTuple(data) != QE_EOF){
		int attrtype = 0;
		int value_int;
		float value_float;
		string value_string;
		find_s_value(attrtype, value_int, value_float, value_string, attr_right_index, data);
		if(attrtype==1){
			int fileindex = value_int % numPartitions;
			RelationManager::instance()->insertTuple("right_join_"+to_string(name_id)+"_"+s_tableName+to_string(fileindex),data,rid);
		}else if(attrtype==2){
			int fileindex = (int)value_float % numPartitions;
			RelationManager::instance()->insertTuple("right_join_"+to_string(name_id)+"_"+s_tableName+to_string(fileindex),data,rid);
		}else{
			int fileindex=(value_string[0]-'!') % numPartitions;
			RelationManager::instance()->insertTuple("right_join_"+to_string(name_id)+"_"+s_tableName+to_string(fileindex),data,rid);
		}
		free(data);
		data=malloc(PAGE_SIZE);
	}
	free(data);
}
RC GHJoin::find_s_value(int &attrtype, int &value_int, float &value_float, string &value_string, int attr_index,void *data) {
	int offset = 0; // offset is the length of record
	int size = attrs_in.size();
	int byte_count = ceil((double)size / 8);
	//calcuate the length of record;
	char* bites = (char*)malloc(byte_count);//the number of fields
	memcpy(bites, (char *)data + offset, byte_count);
	offset = offset + byte_count;
	for (int i = 0; i<size; i++) {
		if (!(bites[i / 8] & (1 << (7 - i % 8)))) {
			if (attrs_in[i].type == TypeVarChar) {
				int string_size;
				memcpy(&string_size, (char *)data + offset, 4);
				offset = offset + string_size + 4;
				if (i == attr_index) {
					attrtype = 3;
					char *value_char = (char *)malloc(string_size + 1);
					memcpy(value_char, (char *)data + offset - string_size, string_size);
					value_char[string_size] = '\0';
					value_string = value_char;
					free(value_char);
					break;
				}
			}
			else if (attrs_in[i].type == TypeInt) {
				if (i == attr_index) {
					memcpy(&value_int, (char *)data + offset, sizeof(int));
					attrtype = 1;
					break;
				}
				offset = offset + 4;
			}
			else {
				if (i == attr_index) {
					memcpy(&value_float, (char *)data + offset, sizeof(float));
					attrtype = 2;
					break;
				}
				offset = offset + 4;
			}
		}

	}
	free(bites);
}
RC GHJoin::map_rPartitions(int r_index){
	RelationManager::instance()->scan("left_join_"+to_string(name_id)+"_"+r_tableName+to_string(r_index),"",NO_OP,NULL,string_vector_out,rm_ite);
		void *data=malloc(PAGE_SIZE);
		//rm_ite.getNextTuple(rid,data);
		
		int attr_index;
		for (int i = 0; i<attrs_out.size(); i++) {
			if (attrs_out[i].name == condition.lhsAttr) {
				attr_index = i;
				break;
			}
		}
		while(rm_ite.getNextTuple(rid,data) != -1){
			int attrtype=0;
			int value_int;
			float value_float;
			string value_string;
		find_r_value(attrtype, value_int, value_float, value_string, attr_index,data);
		if(attrtype==1){
			it1=map1.find(value_int);
			if(it1 != map1.end()){
				int length;
				getByteLength(attrs_out,data,length);
				Tuple sig_tuple(data,length);
				it1->second.push_back(sig_tuple);
			}else{
				vector<Tuple> tuple;
				int length;
				getByteLength(attrs_out,data,length);
				Tuple sig_tuple(data,length);
				tuple.push_back(sig_tuple);
				map1.insert(make_pair(value_int,tuple));
			}
		}else if(attrtype==2){
			it2=map2.find(value_float);
			if(it2 != map2.end()){
				int length;
				getByteLength(attrs_out,data,length);
				Tuple sig_tuple(data,length);
				it2->second.push_back(sig_tuple);
				//cout<<"left vector size"<<it2->second.size()<<endl;
			}else{
				vector<Tuple> tuple;
				int length;
				getByteLength(attrs_out,data,length);
				Tuple sig_tuple(data,length);
				tuple.push_back(sig_tuple);
				map2.insert(make_pair(value_float,tuple));
			}
		}else{
			it3=map3.find(value_string);
			if(it3 != map3.end()){
				int length;
				getByteLength(attrs_out,data,length);
				Tuple sig_tuple(data,length);
				it3->second.push_back(sig_tuple);
			}else{
				vector<Tuple> tuple;
				int length;
				getByteLength(attrs_out,data,length);
				Tuple sig_tuple(data,length);
				tuple.push_back(sig_tuple);
				map3.insert(make_pair(value_string,tuple));
			}
		}
		free(data);
		data=malloc(PAGE_SIZE);
	}
	free(data);
	rm_ite.close();
}
RC GHJoin::getByteLength(vector<Attribute> attrs, void *data, int &size) {
	int offset = 0; // offset is the length of record
	int size1 = attrs.size();
	int byte_count = ceil((double)size1 / 8);

	//calcuate the length of record;
	char* bites = (char*)malloc(byte_count);//the number of fields
	memcpy(bites, (char *)data + offset, byte_count);
	offset = offset + byte_count;
	for (int i = 0; i<size1; i++) {
		if (!(bites[i / 8] & (1 << (7 - i % 8)))) {
			if (attrs[i].type == TypeVarChar) {
				int string_size;
				memcpy(&string_size, (char *)data + offset, 4);
				offset = offset + string_size + 4;
			}
			else {
				offset = offset + 4;
			}
		}
	}
	free(bites);
	size = offset;
}
RC GHJoin::getNextTuple(void *data){
	int attr_right_index;
	for (int i = 0; i<attrs_in.size(); i++) {
		if (attrs_in[i].name == condition.rhsAttr) {
			attr_right_index = i;
			break;
		}
	}
	
	while(true){
		int attrtype = 0;
		int value_int;
		float value_float;
		string value_string;
	find_s_value(attrtype, value_int, value_float, value_string, attr_right_index, data_s);
	if(attrtype==1){
		//cout<<"attr type right:   "<<attrtype<<"value_int right   :"<<value_int<<endl;
		it1=map1.find(value_int);
		if(it1 != map1.end()){
			//cout<<"        outer size      "<<it1->second.size()<<endl;
		if(vector_index<it1->second.size()){
			int size_s;
			getByteLength(attrs_in,data_s,size_s);
			getJoin(data,it1->second[vector_index].data,it1->second[vector_index].length,data_s,size_s);
			vector_index++;
			return 0;
		}else{
			free(data_s);
			data_s=malloc(PAGE_SIZE);
			if(rm_ite.getNextTuple(rid,data_s)!=-1){
				vector_index=0;
			}else{
				rm_ite.close();
				cur_partition++;
				if(cur_partition>=(int)numPartitions){
					// for(int i=0;i<(int)numPartitions;i++){
					// 	RelationManager::instance()->deleteTable("left_join_"+r_tableName+to_string(i));
					// 	RelationManager::instance()->deleteTable("right_join_"+s_tableName+to_string(i));
					// }
					return QE_EOF;
				}else{
					map1.clear();
					map_rPartitions(cur_partition);
					RelationManager::instance()->scan("right_join_"+to_string(name_id)+"_"+s_tableName+to_string(cur_partition),"",NO_OP,NULL,string_vector_in,rm_ite);
					rm_ite.getNextTuple(rid,data_s);
					vector_index=0;
				}
			}
		}
	}else{
		free(data_s);
		data_s=malloc(PAGE_SIZE);
		if(rm_ite.getNextTuple(rid,data_s)!=-1){
				vector_index=0;
			}else{
				rm_ite.close();
				cur_partition++;
				if(cur_partition>=(int)numPartitions){
					// for(int i=0;i<(int)numPartitions;i++){
					// 	RelationManager::instance()->deleteTable("left_join_"+r_tableName+to_string(i));
					// 	RelationManager::instance()->deleteTable("right_join_"+s_tableName+to_string(i));
					// }
					return QE_EOF;
				}else{
					map1.clear();
					map_rPartitions(cur_partition);
					RelationManager::instance()->scan("right_join_"+to_string(name_id)+"_"+s_tableName+to_string(cur_partition),"",NO_OP,NULL,string_vector_in,rm_ite);
					rm_ite.getNextTuple(rid,data_s);
					vector_index=0;
				}
			}
	}
	}else if(attrtype==2){
		//cout<<"                 cur_partition            "<<cur_partition<<"     float   "<<value_float<<endl;
		it2=map2.find(value_float);
		if(it2 != map2.end()){

		if(vector_index< it2->second.size()){
			//cout<<"2"<<endl;
			int size_s;
			getByteLength(attrs_in,data_s,size_s);

			getJoin(data,it2->second[vector_index].data,it2->second[vector_index].length,data_s,size_s);
			vector_index++;
			return 0;
		}else{
			free(data_s);
			data_s=malloc(PAGE_SIZE);
			if(rm_ite.getNextTuple(rid,data_s)!=-1){
				//cout<<"3"<<endl;
				vector_index=0;
			}else{
				rm_ite.close();
				cur_partition++;
				if(cur_partition>=(int)numPartitions){
					// for(int i=0;i<(int)numPartitions;i++){
					// 	RelationManager::instance()->deleteTable("left_join_"+r_tableName+to_string(i));
					// 	RelationManager::instance()->deleteTable("right_join_"+s_tableName+to_string(i));
					// }
					return QE_EOF;
				}else{
					map2.clear();
					map_rPartitions(cur_partition);
					RelationManager::instance()->scan("right_join_"+to_string(name_id)+"_"+s_tableName+to_string(cur_partition),"",NO_OP,NULL,string_vector_in,rm_ite);
					rm_ite.getNextTuple(rid,data_s);
					vector_index=0;
				}
			}
		}
	}else{
		free(data_s);
			data_s=malloc(PAGE_SIZE);
			if(rm_ite.getNextTuple(rid,data_s)!=-1){
				//cout<<"4"<<endl;
				vector_index=0;
			}else{
				rm_ite.close();
				cur_partition++;
				if(cur_partition>=(int)numPartitions){
					// for(int i=0;i<(int)numPartitions;i++){
					// 	RelationManager::instance()->deleteTable("left_join_"+r_tableName+to_string(i));
					// 	RelationManager::instance()->deleteTable("right_join_"+s_tableName+to_string(i));
					// }
					return QE_EOF;
				}else{
					map2.clear();
					map_rPartitions(cur_partition);
					RelationManager::instance()->scan("right_join_"+to_string(name_id)+"_"+s_tableName+to_string(cur_partition),"",NO_OP,NULL,string_vector_in,rm_ite);
					rm_ite.getNextTuple(rid,data_s);
					vector_index=0;
				}
			}
	}
	}else{
		it3=map3.find(value_string);
		if(it3 != map3.end()){
		if(vector_index<it3->second.size()){
			int size_s;
			getByteLength(attrs_in,data_s,size_s);
			getJoin(data,it3->second[vector_index].data,it3->second[vector_index].length,data_s,size_s);
			vector_index++;
			return 0;
		}else{
			free(data_s);
			data_s=malloc(PAGE_SIZE);
			if(rm_ite.getNextTuple(rid,data_s)!=-1){
				vector_index=0;
			}else{
				rm_ite.close();
				cur_partition++;
				if(cur_partition>=(int)numPartitions){
					//free(data_s);
					// for(int i=0;i<(int)numPartitions;i++){
					// 	RelationManager::instance()->deleteTable("left_join_"+r_tableName+to_string(i));
					// 	RelationManager::instance()->deleteTable("right_join_"+s_tableName+to_string(i));
					// }
					return QE_EOF;
				}else{
					map3.clear();
					map_rPartitions(cur_partition);
					RelationManager::instance()->scan("right_join_"+to_string(name_id)+"_"+s_tableName+to_string(cur_partition),"",NO_OP,NULL,string_vector_in,rm_ite);
					rm_ite.getNextTuple(rid,data_s);
					vector_index=0;
				}
			}
		}
	}else{
		free(data_s);
			data_s=malloc(PAGE_SIZE);
			if(rm_ite.getNextTuple(rid,data_s)!=-1){
				vector_index=0;
			}else{
				rm_ite.close();
				cur_partition++;
				if(cur_partition>=(int)numPartitions){
					//free(data_s);
					// for(int i=0;i<(int)numPartitions;i++){
					// 	RelationManager::instance()->deleteTable("left_join_"+r_tableName+to_string(i));
					// 	RelationManager::instance()->deleteTable("right_join_"+s_tableName+to_string(i));
					// }
					return QE_EOF;
				}else{
					map3.clear();
					map_rPartitions(cur_partition);
					RelationManager::instance()->scan("right_join_"+to_string(name_id)+"_"+s_tableName+to_string(cur_partition),"",NO_OP,NULL,string_vector_in,rm_ite);
					rm_ite.getNextTuple(rid,data_s);
					vector_index=0;
				}
			}
	}
	}

}
}
RC GHJoin::getJoin(void *data, void *r_data, int outer_length, void *s_data, int inner_length) {
	int offset = 0; // offset is the length of record
	int size = attrs_out.size();
	int byte_count = ceil((double)size / 8);
	char* bites = (char*)malloc(byte_count);//the number of fields
	memcpy(bites, (char *)r_data + offset, byte_count);

	int offset1 = 0;
	int size1 = attrs_in.size();
	int byte_count1 = ceil((double)size1 / 8);
	char* bites1 = (char*)malloc(byte_count1);//the number of fields
	memcpy(bites1, (char *)s_data + offset1, byte_count1);


	int join_size = size + size1;
	int join_byte_count = ceil((double)join_size / 8);
	//calcuate the length of record;
	int join_offset = join_byte_count;

	memcpy((char *)data, bites, byte_count);
	for (int i = size; i<join_size; i++) {
		if (!(bites1[(i - size) / 8] & (1 << (7 - (i - size) % 8))));
		else {
			((char *)data)[i / 8] = ((char *)data)[i / 8] | (1 << (7 - (i - size) % 8));
		}
	}
	memcpy((char *)data + join_offset, (char *)r_data + byte_count, outer_length - byte_count);
	memcpy((char *)data + join_offset + outer_length - byte_count, (char *)s_data + byte_count1, inner_length - byte_count1);
	free(bites);
	free(bites1);
}
void GHJoin::getAttributes(vector<Attribute> &attrs) const{
	for (int i = 0; i<attrs_out.size(); i++) {
		attrs.push_back(attrs_out[i]);
	}
	for (int i = 0; i<attrs_in.size(); i++) {
		attrs.push_back(attrs_in[i]);
	}
}
