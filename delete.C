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
		return BADCATPARM;
	}

	HeapFileScan *heapScanner;
	Status opStatus;
	AttrDesc attributeDetails;
	RID recordID;
	int intVal;
	float floatVal;
	cout << "Delete!" << endl;
	heapScanner = new HeapFileScan(relation, opStatus);
	if (opStatus != OK)
	{
		return opStatus;
	}

	if (attrName.empty())
	{
		opStatus = heapScanner->startScan(0, 0, STRING, nullptr, EQ);
	}
	else
	{
		opStatus = attrCat->getInfo(relation, attrName, attributeDetails);
		if (opStatus != OK)
		{
			delete heapScanner;
			return opStatus;
		}

		switch (type)
		{
		case INTEGER:
			intVal = atoi(attrValue);
			opStatus = heapScanner->startScan(attributeDetails.attrOffset, attributeDetails.attrLen, INTEGER, reinterpret_cast<const char *>(&intVal), op);
			break;
		case FLOAT:
			floatVal = atof(attrValue);
			opStatus = heapScanner->startScan(attributeDetails.attrOffset, attributeDetails.attrLen, FLOAT, reinterpret_cast<const char *>(&floatVal), op);
			break;
		default:
			opStatus = heapScanner->startScan(attributeDetails.attrOffset, attributeDetails.attrLen, STRING, attrValue, op);
		}
	}

	if (opStatus != OK)
	{
		delete heapScanner;
		return opStatus;
	}

	while ((opStatus = heapScanner->scanNext(recordID)) == OK)
	{
		opStatus = heapScanner->deleteRecord();
		if (opStatus != OK)
		{
			delete heapScanner;
			return opStatus;
		}
	}

	heapScanner->endScan();
	delete heapScanner;

	return (opStatus == FILEEOF) ? OK : opStatus;
}
