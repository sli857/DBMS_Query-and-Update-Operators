#include "catalog.h"
#include "query.h"

// forward declaration
const Status ScanSelect(const string &result,
						const int projCnt,
						const AttrDesc projNames[],
						const AttrDesc *attrDesc,
						const Operator op,
						const char *filter,
						const int reclen);

/*
 * Selects records from the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Select(const string &result,
					   const int projCnt,
					   const attrInfo projNames[],
					   const attrInfo *attr,
					   const Operator op,
					   const char *attrValue)
{
	if (projCnt == 0)
	{
		return OK;
	}

	AttrDesc resRecords[projCnt];
	AttrDesc attrDesc;
	Status status;
	for (int i = 0; i < projCnt; i++)
	{
		status = attrCat->getInfo(projNames[i].relName, projNames[i].attrName, resRecords[i]);
		if (status != OK)
		{
			return status;
		}
	}

	if (attr != NULL)
	{
		status = attrCat->getInfo(attr->relName, attr->attrName, attrDesc);
	}
	else
	{
		attrDesc = resRecords[0];
	}

	if (status != OK)
	{
		return status;
	}

	int reclen = 0;
	for (int i = 0; i < projCnt; i++)
	{
		reclen += resRecords[i].attrLen;
	}

	void *dest;
	int temInt;
	float temFloat;
	if (attrValue == NULL)
	{
		dest = NULL;
	}
	else
	{
		switch (attr->attrType)
		{
		case STRING:
			status = ScanSelect(result, projCnt, resRecords, &attrDesc, op, attrValue, reclen);
			return status;
			break;
		case INTEGER:
			temInt = atoi(attrValue);
			dest = (void *)(&temInt);
			break;
		case FLOAT:
			temFloat = atof(attrValue);
			dest = (void *)(&temFloat);
			break;
		default:
			// memcpy(dest, attrValue, attr->attrLen);
			break;
		}
	}

	status = ScanSelect(result, projCnt, resRecords, &attrDesc, op, (char *)dest, reclen);

	return status;
}

const Status ScanSelect(const string &result,
#include "stdio.h"
#include "stdlib.h"
						const int projCnt,
						const AttrDesc projNames[],
						const AttrDesc *attrDesc,
						const Operator op,
						const char *filter,
						const int reclen)
{

	Status status;
	InsertFileScan insertionScan(result, status);
	if (status != OK)
	{
		return status;
	}

	char tmpData[reclen];
	Record outrec = {.data=(void*) tmpData, .length=reclen};

	if (attrDesc == NULL)
	{
		return OK;
	}
	HeapFileScan hfScan(std::string(attrDesc->relName), status);
	if (status != OK)
	{
		return status;
	}

	int offset = attrDesc->attrOffset;
	int len = attrDesc->attrLen;
	auto type = (Datatype) attrDesc->attrType;
	status = hfScan.startScan(offset, len, type, filter, op);

	if (status != OK)
	{
		return status;
	}

	RID rid;
	Record tmpRec;
	while ((status=hfScan.scanNext(rid)) == OK)
	{
		status = hfScan.getRecord(tmpRec);
		if (status != OK)
		{
			return status;
		}

		int offset = 0;
		for (int i = 0; i < projCnt; i++)
		{
			memcpy(tmpData + offset,
				   (char *)tmpRec.data + projNames[i].attrOffset,
				   projNames[i].attrLen);
			offset += projNames[i].attrLen;
		}

		RID outRID;
		status = insertionScan.insertRecord(outrec, outRID);
		if (status != OK)
		{
			return status;
		}
	}
	return status==FILEEOF? OK:status;
}