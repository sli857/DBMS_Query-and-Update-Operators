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
	cout << "Doing HeapFileScan Selection using ScanSelect()" << endl;

	Status status;
	InsertFileScan insertionScan(result, status);
	if (status != OK)
	{
		return status;
	}

	char data[reclen];
	Record outrec;
	outrec.data = (void *)data;
	outrec.length = reclen;
	if (attrDesc == NULL)
	{
		return OK;
	}
	HeapFileScan fileScan(string(attrDesc->relName), status);
	if (status != OK)
	{
		return status;
	}

	status = fileScan.startScan(attrDesc->attrOffset, attrDesc->attrLen, (Datatype)attrDesc->attrType, filter, op);
	if (status != OK)
	{
		return status;
	}

	RID rid;
	Record temrec;
	while (fileScan.scanNext(rid) == OK)
	{

		status = fileScan.getRecord(temrec);
		if (status != OK)
		{
			return status;
		}

		int offset = 0;

		for (int i = 0; i < projCnt; i++)
		{
			memcpy(data + offset,
				   (char *)temrec.data + projNames[i].attrOffset,
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
	return OK;
}