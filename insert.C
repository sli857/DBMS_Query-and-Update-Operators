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
	RID tupleID;
	AttrDesc *schemaAttributes;
	int schemaAttrCount;
	int recordSize = 0;
	char *recordData;
	cout << "Insert!" << endl;
	status = attrCat->getRelInfo(relation, schemaAttrCount, schemaAttributes);
	if (status != OK)
	{
		return status;
	}

	if (schemaAttrCount != attrCnt)
	{
		return BADSCANPARM;
	}

	for (int i = 0; i < schemaAttrCount; ++i)
	{
		recordSize += schemaAttributes[i].attrLen;
	}

	InsertFileScan insertionScan(relation, status);
	if (status != OK)
	{
		return status;
	}

	recordData = (char *)malloc(recordSize);
	if (!recordData)
	{
		return INSUFMEM;
	}

	memset(recordData, 0, recordSize);

	bool attrFound;
	for (int i = 0; i < schemaAttrCount; ++i)
	{
		attrFound = false;
		for (int j = 0; j < attrCnt; ++j)
		{
			if (strcmp(attrList[j].attrName, schemaAttributes[i].attrName) == 0)
			{
				attrFound = true;
				if (attrList[j].attrType != schemaAttributes[i].attrType)
				{
					free(recordData);
					return ATTRTYPEMISMATCH;
				}
				void *dest = recordData + schemaAttributes[i].attrOffset;
				memcpy(dest, attrList[j].attrValue, schemaAttributes[i].attrLen);
				break;
			}
		}
		if (!attrFound)
		{
			free(recordData);
			return ATTRNOTFOUND;
		}
	}

	Record newRecord = {recordData, recordSize};
	status = insertionScan.insertRecord(newRecord, tupleID);
	free(recordData);

	return status;
}
