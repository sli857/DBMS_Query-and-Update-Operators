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
	// Qu_Select sets up things and then calls ScanSelect to do the actual work
	cout << "Doing QU_Select " << endl;
	if (projCnt == 0)
	{
		return OK;
	}
	// cout << "Select test point 1" << endl;
	AttrDesc adProjNames[projCnt];
	AttrDesc attrDesc;
	Status status;
	int reclen;
	for (int i = 0; i < projCnt; i++)
	{
		status = attrCat->getInfo(projNames[i].relName, projNames[i].attrName, adProjNames[i]);
		if (status != OK)
		{
			return status;
		}
	}
	// cout << "Select test point 2" << endl;
	if (attr != NULL)
	{
		status = attrCat->getInfo(attr->relName, attr->attrName, attrDesc);
	}
	else
	{
		attrDesc = adProjNames[0];
	}
	// cout << "Select test point 2.5" << endl;
	if (status != OK)
	{
		return status;
	}
	// cout << "Select test point 3" << endl;
	reclen = 0;
	for (int i = 0; i < projCnt; i++)
	{
		reclen += adProjNames[i].attrLen;
	}
	// cout << "Select test point 4, type: " << endl;
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
			status = ScanSelect(result, projCnt, adProjNames, &attrDesc, op, attrValue, reclen);
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
	// cout << "Select test point 5" << endl;
	status = ScanSelect(result, projCnt, adProjNames, &attrDesc, op, (char *)dest, reclen);
	// cout << "Select test point 6" << endl;
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
    if (status != OK){ 
      return status; 
    }
    //cout << "ScanSelect test point 1" << endl;
    char data[reclen];
    Record outrec;
    outrec.data = (void *) data;
    outrec.length = reclen;
    if(attrDesc == NULL){
      return OK;
    }
    HeapFileScan fileScan(string(attrDesc->relName), status);
    if (status != OK){ 
      return status; 
    }
    //cout << "ScanSelect test point 2" << endl;
    status = fileScan.startScan(attrDesc->attrOffset, attrDesc->attrLen, (Datatype)attrDesc->attrType, filter, op);
    if (status != OK){
      return status; 
    }
    //cout << "ScanSelect test point 3" << endl;
    RID rid;
    Record temrec;
    while (fileScan.scanNext(rid) == OK){
        //cout << "ScanSelect test point 4_1" << endl;
        status = fileScan.getRecord(temrec);
        if (status != OK){
          //cout << "ScanSelect error 3" << endl;
          return status; 
        }

        int offset = 0;
        //cout << "ScanSelect test point 4_2" << endl;
        for (int i = 0; i < projCnt; i++){
            memcpy(data + offset,
                        (char *)temrec.data + projNames[i].attrOffset,
                        projNames[i].attrLen);                    
            offset += projNames[i].attrLen;
        } 
        RID outRID;
        //cout << "ScanSelect test point 4_3" << endl;
        status = insertionScan.insertRecord(outrec, outRID);
        if (status != OK){
          //cout << "ScanSelect error 4" << endl;
          return status; 
        }
        //cout << "ScanSelect test point 4_4" << endl;
    }
    return OK;
}
