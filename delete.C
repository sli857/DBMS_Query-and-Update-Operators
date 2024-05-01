#include "catalog.h"
#include "query.h"

/*
 * Deletes records from a specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Delete(const string &relation,
					   const string &attrName,
					   const Operator op,
					   const Datatype type,
					   const char *attrValue)
{
	if (relation.empty())
	{
		return ATTRNOTFOUND;
	}

	Status status;
	AttrDesc attrDesc;
	RID rid;

	HeapFileScan *heapFileScan = new HeapFileScan(relation, status);
	if (status != OK)
	{
		return status;
	}

	if (attrName.empty())
	{
		status = heapFileScan->startScan(0, 0, STRING, nullptr, EQ);
	}
	else
	{
		status = attrCat->getInfo(relation, attrName, attrDesc);
		if (status != OK)
		{
			delete heapFileScan;
			return status;
		}

		switch (type)
		{
					case (STRING):
		{
			status = heapFileScan->startScan(attrDesc.attrOffset, attrDesc.attrLen, STRING, attrValue, op);
		}
		case (INTEGER):
		{
			int intVal = atoi(attrValue);
			status = heapFileScan->startScan(attrDesc.attrOffset, attrDesc.attrLen, INTEGER, (char *)(&intVal), op);
			break;
		}
		case (FLOAT):
		{
			float floatVal = atof(attrValue);
			status = heapFileScan->startScan(attrDesc.attrOffset, attrDesc.attrLen, FLOAT, (char *)(&floatVal), op);
			break;
		}

		}
	}

	if (status != OK)
	{
		delete heapFileScan;
		return status;
	}

	while ((status = heapFileScan->scanNext(rid)) == OK)
	{
		if ((status = heapFileScan->deleteRecord()) != OK)
		{
			delete heapFileScan;
			return status;
		}
	}

	heapFileScan->endScan();
	delete heapFileScan;

	return (status == FILEEOF) ? OK : status;
}
