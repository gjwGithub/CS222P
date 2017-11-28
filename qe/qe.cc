
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
	int offset = 0;
	for (int i = 0; i < index; i++)
	{
		if (attrs[i].type == AttrType::TypeInt)
			offset += sizeof(int);
		else if (attrs[i].type == AttrType::TypeReal)
			offset += sizeof(float);
		else if (attrs[i].type == AttrType::TypeVarChar)
			offset += sizeof(int) + *(int*)((char*)data + offset);
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
			return a >= b;
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
			return a >= b;
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
			return a >= b;
		else if (op == CompOp::NE_OP)
			return a != b;
		else
			return true;
	}
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
		int nullIndicatorSize = ceil(this->attrs.size() / CHAR_BIT);
		int offset = nullIndicatorSize;
		int count = 0;
		for (size_t i = 0; i < totalAttrsCount; i++, count++)
		{
			unsigned char nullFields = ((unsigned char*)this->buffer)[count / CHAR_BIT];
			bool isNULL = ((unsigned char*)this->buffer)[count / CHAR_BIT] & (1 << (CHAR_BIT - 1 - count % CHAR_BIT));
			if ((int)i != this->attrIndexes[count])
			{
				if (!isNULL)
				{
					if (attrs[count].type == AttrType::TypeInt)
						offset += sizeof(int);
					else if (attrs[count].type == AttrType::TypeReal)
						offset += sizeof(float);
					else if (attrs[count].type == AttrType::TypeVarChar)
					{
						int strLength = *(int*)(this->buffer + offset);
						offset += sizeof(int) + strLength;
					}
				}
				continue;
			}
			else
			{
				if (isNULL)
					nullFields += 1 << (7 - count % 8);
				else
				{
					nullFields += 0 << (7 - count % 8);
					if (attrs[count].type == AttrType::TypeInt)
					{
						memcpy((char*)data + offset, this->buffer + offset, sizeof(int));
						offset += sizeof(int);
					}
					else if (attrs[count].type == AttrType::TypeReal)
					{
						memcpy((char*)data + offset, this->buffer + offset, sizeof(float));
						offset += sizeof(float);
					}
					else if (attrs[count].type == AttrType::TypeVarChar)
					{
						int strLength = *(int*)(this->buffer + offset);
						memcpy((char*)data + offset, this->buffer + offset, sizeof(int) + strLength);
						offset += sizeof(int) + strLength;
					}
				}
				((unsigned char*)data)[count / CHAR_BIT] = nullFields;
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
	{
		this->end = true;
	}
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
	int nullIndicatorSize = ceil((this->leftAttrs.size() + this->rightAttrs.size()) / CHAR_BIT);
	int offset = nullIndicatorSize;
	//Copy left attributes
	int leftOffset = ceil(this->leftAttrs.size() / CHAR_BIT);
	for (size_t i = 0; i < this->leftAttrs.size(); i++)
	{
		unsigned char nullFields = ((unsigned char*)this->leftBuffer)[i / CHAR_BIT];
		bool isNULL = this->leftBuffer[i / CHAR_BIT] & (1 << (CHAR_BIT - 1 - i % CHAR_BIT));
		if (isNULL)
			nullFields += 1 << (7 - i % 8);
		else
		{
			nullFields += 0 << (7 - i % 8);
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
	int rightOffset = ceil(this->rightAttrs.size() / CHAR_BIT);
	for (size_t i = 0; i < this->rightAttrs.size(); i++)
	{
		unsigned char nullFields = ((unsigned char*)this->rightBuffer)[i / CHAR_BIT];
		bool isNULL = this->rightBuffer[i / CHAR_BIT] & (1 << (CHAR_BIT - 1 - i % CHAR_BIT));
		if (isNULL)
			nullFields += 1 << (7 - (i + this->rightAttrs.size()) % 8);
		else
		{
			nullFields += 0 << (7 - (i + this->rightAttrs.size()) % 8);
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
		((unsigned char*)data)[(i + this->rightAttrs.size()) / CHAR_BIT] = nullFields;
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
}

RC Aggregate::getNextTuple(void *data)
{
	((char*)data)[0] = 0;
	switch (this->op)
	{
	case AggregateOp::AVG:
	{
		char* temp = (char*)malloc(PAGE_SIZE);
		double sum = 0;
		int count = 0;
		while (this->input->getNextTuple(temp))
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
		double result = sum / count;
		if (this->aggAttr.type == AttrType::TypeInt)
		{
			int value = (int)result;
			memcpy((char*)data + 1, &value, sizeof(int));
		}
		else if (this->aggAttr.type == AttrType::TypeReal)
		{
			float value = (float)result;
			memcpy((char*)data + 1, &value, sizeof(float));
		}
		free(temp);
	}
	break;
	case AggregateOp::COUNT:
	{
		char* temp = (char*)malloc(PAGE_SIZE);
		int count = 0;
		while (this->input->getNextTuple(temp))
			++count;
		memcpy((char*)data + 1, &count, sizeof(float));
		free(temp);
	}
	break;
	case AggregateOp::MAX:
	{
		char* temp = (char*)malloc(PAGE_SIZE);
		int maxInt = numeric_limits<int>::min();
		float maxFloat = numeric_limits<float>::min();
		while (this->input->getNextTuple(temp))
		{
			Value value = getAttributeValue(temp, this->attrIndex, this->attrs);
			if (value.type == AttrType::TypeInt)
				maxInt = *(int*)value.data > maxInt ? *(int*)value.data : maxInt;
			else if (value.type == AttrType::TypeReal)
				maxFloat = *(int*)value.data > maxFloat ? *(int*)value.data : maxFloat;
		}
		if (this->aggAttr.type == AttrType::TypeInt)
			memcpy((char*)data + 1, &maxInt, sizeof(int));
		else if (this->aggAttr.type == AttrType::TypeReal)
			memcpy((char*)data + 1, &maxFloat, sizeof(float));
		free(temp);
	}
	break;
	case AggregateOp::MIN:
	{
		char* temp = (char*)malloc(PAGE_SIZE);
		int minInt = numeric_limits<int>::max();
		float minFloat = numeric_limits<float>::max();
		while (this->input->getNextTuple(temp))
		{
			Value value = getAttributeValue(temp, this->attrIndex, this->attrs);
			if (value.type == AttrType::TypeInt)
				minInt = *(int*)value.data < minInt ? *(int*)value.data : minInt;
			else if (value.type == AttrType::TypeReal)
				minFloat = *(int*)value.data < minFloat ? *(int*)value.data : minFloat;
		}
		if (this->aggAttr.type == AttrType::TypeInt)
			memcpy((char*)data + 1, &minInt, sizeof(int));
		else if (this->aggAttr.type == AttrType::TypeReal)
			memcpy((char*)data + 1, &minFloat, sizeof(float));
		free(temp);
	}
	break;
	case AggregateOp::SUM:
	{
		char* temp = (char*)malloc(PAGE_SIZE);
		double sum = 0;
		while (this->input->getNextTuple(temp))
		{
			Value value = getAttributeValue(temp, this->attrIndex, this->attrs);
			if (value.type == AttrType::TypeInt)
				sum += *(int*)value.data;
			else if (value.type == AttrType::TypeReal)
				sum += *(float*)value.data;
		}
		if (this->aggAttr.type == AttrType::TypeInt)
		{
			int value = (int)sum;
			memcpy((char*)data + 1, &value, sizeof(int));
		}
		else if (this->aggAttr.type == AttrType::TypeReal)
		{
			float value = (float)sum;
			memcpy((char*)data + 1, &value, sizeof(float));
		}
		free(temp);
	}
	break;
	default:
		break;
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