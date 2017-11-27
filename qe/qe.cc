
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
	if (!end)
	{
		while (input->getNextTuple(data) != QE_EOF)
		{
			if (condition.op == NO_OP)
				return 0;
			Value leftValue = getAttributeValue(data, this->leftIndex, this->attrs);
			Value rightValue;
			if (condition.bRhsIsAttr)
				rightValue = getAttributeValue(data, this->rightIndex, this->attrs);
			else
				rightValue = condition.rhsValue;
			if (compareAttributes(leftValue.data, rightValue.data, this->condition.op, leftValue.type))
				return 0;
		}
		end = true;
	}
	return QE_EOF;
}
// ... the rest of your implementations go here

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
	unsigned char* nullIndicator = (unsigned char*)malloc(nullIndicatorSize);
	int offset = nullIndicatorSize;
	//Copy left attributes
	int leftOffset = ceil(this->leftAttrs.size() / CHAR_BIT);
	for (size_t i = 0; i < this->leftAttrs.size(); i++)
	{
		unsigned char nullFields = ((unsigned char*)data)[i / CHAR_BIT];
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
		unsigned char nullFields = ((unsigned char*)data)[(i + this->leftAttrs.size()) / CHAR_BIT];
		bool isNULL = this->rightBuffer[i / CHAR_BIT] & (1 << (CHAR_BIT - 1 - i % CHAR_BIT));
		if (isNULL)
			nullFields += 1 << (7 - (i + this->leftAttrs.size()) % 8);
		else
		{
			nullFields += 0 << (7 - (i + this->leftAttrs.size()) % 8);
			if (rightAttrs[i].type == AttrType::TypeInt)
			{
				memcpy((char*)data + offset, leftBuffer + leftOffset, sizeof(int));
				offset += sizeof(int);
				leftOffset += sizeof(int);
			}
			else if (rightAttrs[i].type == AttrType::TypeReal)
			{
				memcpy((char*)data + offset, leftBuffer + leftOffset, sizeof(float));
				offset += sizeof(float);
				leftOffset += sizeof(float);
			}
			else if (rightAttrs[i].type == AttrType::TypeVarChar)
			{
				int strLength = *(int*)(leftBuffer + leftOffset);
				memcpy((char*)data + offset, leftBuffer + leftOffset, sizeof(int) + strLength);
				offset += sizeof(int) + strLength;
				leftOffset += sizeof(int) + strLength;
			}
		}
		((unsigned char*)data)[(i + this->leftAttrs.size()) / CHAR_BIT] = nullFields;
	}
	return 0;
}
