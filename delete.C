#include "catalog.h"
#include "query.h"


/*
 * Deletes records from a specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Delete(const string & relation, 
		       const string & attrName, 
		       const Operator op,
		       const Datatype type, 
		       const char *attrValue)
{
	Status status;
	RID rid;
	AttrDesc attrDesc;

  	HeapFileScan* heapFileScan = new HeapFileScan(relation, status);
  	if(status != OK) {
  		return status;
	}
  	attrCat->getInfo(relation, attrName, attrDesc);

	int offset = attrDesc.attrOffset;
	int length = attrDesc.attrLen;

	int intValue;
	float floatValue;

	switch(type) {
		case STRING:
			status = heapFileScan->startScan(offset, length, type, attrValue, op);
			break;
	
		case INTEGER:
		 	intValue = atoi(attrValue);
			status = heapFileScan->startScan(offset, length, type, (char *)&intValue, op);
			break;
	
		case FLOAT:
			floatValue = atof(attrValue);
			status = heapFileScan->startScan(offset, length, type, (char *)&floatValue, op);
			break;
	}
	
  	if (status != OK) {
    	delete heapFileScan;
    	return status;
  	}

  	while((status = heapFileScan->scanNext(rid)) == OK) {
    	if ((status = heapFileScan->deleteRecord()) != OK){
			return status;
		}
  	}

	heapFileScan->endScan();
    delete heapFileScan;

  	return OK;

}


