#include "catalog.h"
#include "query.h"

/*
 * Inserts a record into the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Insert(const string &relation,
					   const int attrCnt,
					   const attrInfo attrList[])
{
	Status status;
	AttrDesc *attrDesc;
	int attrCount;

	status = attrCat->getRelInfo(relation, attrCount, attrDesc);
	if (status != OK)
	{
		return status;
	}

	if (attrCount != attrCnt)
	{
		return BADSCANPARM;
	}

	int recordSize = 0;
	for (int i = 0; i < attrCount; ++i)
	{
		recordSize += attrDesc[i].attrLen;
	}

	InsertFileScan insertionScan(relation, status);
	if (status != OK)
	{
		return status;
	}

	char *recordData = (char *)malloc(recordSize);
	if (!recordData)
	{
		return UNIXERR;
	}

	memset(recordData, 0, recordSize);

	bool attrFound;
	for (int i = 0; i < attrCount; ++i)
	{
		attrFound = false;
		for (int j = 0; j < attrCnt; ++j)
		{
			if (strcmp(attrList[j].attrName, attrDesc[i].attrName) == 0)
			{
				attrFound = true;
				if (attrList[j].attrType != attrDesc[i].attrType)
				{
					delete recordData;
					return ATTRTYPEMISMATCH;
				}
				void *dest = recordData + attrDesc[i].attrOffset;
				memcpy(dest, attrList[j].attrValue, attrDesc[i].attrLen);
				break;
			}
		}
		if (!attrFound)
		{
			delete recordData;
			return ATTRNOTFOUND;
		}
	}

	RID rid;
	Record newRecord = {recordData, recordSize};
	status = insertionScan.insertRecord(newRecord, rid);
	delete recordData;

	return status;
}