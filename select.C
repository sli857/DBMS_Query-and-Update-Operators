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

	if (attrDesc == nullptr)
	{
		return OK;
	}

	if(projNames == nullptr){
		return BADCATPARM;
	}

	Status status;
	HeapFileScan hfScan(std::string(attrDesc->relName), status);
	if (status != OK)
	{
		return status;
	}

	int offset = attrDesc->attrOffset;
	int len = attrDesc->attrLen;
	auto type = (Datatype)attrDesc->attrType;
	status = hfScan.startScan(offset, len, type, filter, op);

	if (status != OK)
	{
		return status;
	}

	RID rid;
	Record tmpRec;
	char tmpData[reclen];

	while ((status = hfScan.scanNext(rid)) == OK)
	{
		status = hfScan.getRecord(tmpRec);
		if (status != OK)
		{
			return status;
		}

		// store head address of tmpData
		void *tmpDataHead = &tmpData;
		for (int i = 0; i < projCnt; i++)
		{
			memmove(tmpDataHead,
					(char *)tmpRec.data + projNames[i].attrOffset,
					projNames[i].attrLen);
			// increment address
			tmpDataHead += projNames[i].attrLen;
		}

		InsertFileScan insertionScan(result, status);
		if (status != OK)
		{
			return status;
		}

		RID resultRID;
		Record resultRec = {.data = (void *)tmpData, .length = reclen};
		if ((status = insertionScan.insertRecord(resultRec, resultRID)) != OK)
		{
			return status;
		}
	}

	// Expect FILEEOF
	return status == FILEEOF ? OK : status;
}