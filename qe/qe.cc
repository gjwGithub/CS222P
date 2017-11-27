
#include "qe.h"

Filter::Filter(Iterator* input, const Condition &condition) {
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

int INLJoin::getAttrIndex(const vector<Attribute> &attrs, const string &attr)
{
	for (size_t i = 0; i < attrs.size(); i++)
	{
		if (attrs[i].name == attr)
			return i;
	}
	return -1;
}

Value INLJoin::getAttributeValue(const void* data, const int index, const vector<Attribute> &attrs)
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
	{
		outputJoinResult(data);
		return 0;
	}
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
}
