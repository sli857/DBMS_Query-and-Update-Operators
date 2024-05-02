#include "catalog.h"
#include "query.h"


/*
 * Inserts a record into the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Insert(const string & relation, 
	const int attrCnt, 
	const attrInfo attrList[])
{
	AttrDesc *attrs;
  	int currentAttrCnt;
  	Status status;

  	if((status = attrCat->getRelInfo(relation, currentAttrCnt, attrs)) != OK) {
    	return status;
	}

	if(currentAttrCnt != attrCnt) {
		return UNIXERR;
	}
	
  	int recLen = 0;
  	for(int i = 0; i < attrCnt; i++) {
    	recLen += attrs[i].attrLen;
	}

  	InsertFileScan insertFileScan(relation, status);
  	ASSERT(status == OK);

  	char *insertData;
  	if(!(insertData = new char [recLen])) {
  		return INSUFMEM;
	}

	int insertOffset = 0;
	int intValue = 0;
	float floatValue = 0;
	for(int i = 0; i < attrCnt; i++) {
		bool attrFound = false;
		for(int j = 0; j < attrCnt; j++) {
			if(strcmp(attrs[i].attrName, attrList[j].attrName) == 0) {
				insertOffset = attrs[i].attrOffset;
			
				switch(attrList[j].attrType) {
					case STRING: 
						memcpy((char *)insertData + insertOffset, (char *)attrList[j].attrValue, attrs[i].attrLen);
						break;
			 		
					case INTEGER: 
						intValue = atoi((char *)attrList[j].attrValue);
				 		memcpy((char *)insertData + insertOffset, &intValue, attrs[i].attrLen);
				 		break;
			 		
					case FLOAT: 
						floatValue = atof((char *)attrList[j].attrValue);		
						memcpy((char *)insertData + insertOffset, &floatValue, attrs[i].attrLen);
				 		break;
				}
			
				attrFound = true;
				break;
			}
		}
	
		if(attrFound == false) {
			delete [] insertData;
			free(attrs);
			return UNIXERR;
		}
	}

  	Record insertRec;
  	insertRec.data = (void *) insertData;
  	insertRec.length = recLen;

  	RID insertRID;
  	status = insertFileScan.insertRecord(insertRec, insertRID);

	delete [] insertData;
	free(attrs);

	return status;

}

