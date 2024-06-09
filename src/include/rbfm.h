#ifndef _rbfm_h_
#define _rbfm_h_

#include <vector>
#include <stdio.h>
#include <iostream>
#include <cstring>
#include <sstream>
#include <cmath>
#include <math.h>
#include <algorithm>
#include <set>
#include "pfm.h"

#define PASS 0
#define FAILED -1
#define MOVETONEXT 1
#define DELTA 0.001f



//#ifndef _pfm_ins_h_
//#define _pfm_ins_h_
//PeterDB::PagedFileManager *pfmManagerPtr = &PeterDB::PagedFileManager::instance();
//#endif

namespace PeterDB {
//    PagedFileManager *pfmManagerPtr = &PagedFileManager::instance();
    // Record ID
    typedef struct {
        unsigned pageNum;           // page number
        unsigned short slotNum;     // slot number in the page
    } RID;


    typedef struct {
        char tombStoneByte;           // page number
        int recordStartOffset;     // slot number in the page
        int recordSize;
        int numSlotsUsed;
        int freeSpace;

    } PageMetaData;

    //Tomb stone bit mapping enum
    // 00 = Not a linked list node
    // 01 = linked list start
    // 10 = linked list middle
    // 11 = linked list end
    typedef enum {
        TombStone = 0, LinkedListBit1, LinkedListBit2, NodeDeletionIndicator
    } TombStoneBit;


    //Tomb stone bit mapping enum
    // 00 = Not a linked list node
    // 01 = linked list start
    // 10 = linked list middle
    // 11 = linked list end
    typedef enum {
        NotLinkedListNode = 0, LinkedListStart = 1, LinkedListMiddle = 2, LinkedListEnd = 3
    } LinkedListPosition;

    // Attribute
    typedef enum {
        TypeInt = 0, TypeReal, TypeVarChar
    } AttrType;

    typedef unsigned AttrLength;

    typedef struct Attribute {
        std::string name;  // attribute name
        AttrType type;     // attribute type
        AttrLength length; // attribute length
    } Attribute;

    // Comparison Operator (NOT needed for part 1 of the project)
    typedef enum {
        EQ_OP = 0, // no condition// =
        LT_OP,      // <
        LE_OP,      // <=
        GT_OP,      // >
        GE_OP,      // >=
        NE_OP,      // !=
        NO_OP       // no condition
    } CompOp;


    /********************************************************************
    * The scan iterator is NOT required to be implemented for Project 1 *
    ********************************************************************/

# define RBFM_EOF (-1)  // end of a scan operator

    //  RBFM_ScanIterator is an iterator to go through records
    //  The way to use it is like the following:
    //  RBFM_ScanIterator rbfmScanIterator;
    //  rbfm.open(..., rbfmScanIterator);
    //  while (rbfmScanIterator(rid, data) != RBFM_EOF) {
    //    process the data;
    //  }
    //  rbfmScanIterator.close();
    class RecordBasedFileManager;

    class RBFM_ScanIterator {
    public:
        FileHandle fileHandle;
        std::vector<Attribute> recordDescriptor;
        std::string conditionAttribute;
        const void *expectedValueRecord;
        std::vector<std::string> requestedAttributeNames;
        CompOp compOp;
        void *valueInIterator = nullptr;
        int valueLength;
        AttrType valueInIteratorType;
        std::vector<int> filteredNeededAttributeList;
        int filterAttrIndex;
        unsigned pageNum;
        unsigned short slotNum;
        RecordBasedFileManager* rbfManagerPtr;
        int totalSlotsInCurrentPage;
        char * presentPage;
        int totalNumPageInFile;
        RBFM_ScanIterator();

        ~RBFM_ScanIterator();

        // Never keep the results in the memory. When getNextRecord() is called,
        // a satisfying record needs to be fetched from the file.
        // "data" follows the same format as RecordBasedFileManager::insertRecord().
        RC getNextRecord(RID &rid, void *data);

        bool recordCompare(const void *expectedRecord);

        bool IsnullBitMapSet(char *nullBitMap);

        RC floatCompare(float actual, float expected);

        bool handleEmptyExpectedRecordComparision(void *Value);

        void populateIterator(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                              const std::string &conditionAttribute, const CompOp compOp, const void *value,
                              const std::vector<std::string> &attributeNames);

        bool handleNullMembers( char *expectedRecordDataPtr);
        RC prepareSelectedAttributesRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                           const RID &rid, void *data, std::vector<int> filteredNeededAttributeList);

        RC updateIterator();
        RC close();
    };

    class RecordBasedFileManager {
    public:
        static RecordBasedFileManager &instance();                          // Access to the singleton instance

        RC createFile(const std::string &fileName);                         // Create a new record-based file

        RC destroyFile(const std::string &fileName);                        // Destroy a record-based file

        RC openFile(const std::string &fileName, FileHandle &fileHandle);   // Open a record-based file

        RC closeFile(FileHandle &fileHandle);                               // Close a record-based file

        //  Format of the data passed into the function is the following:
        //  [n byte-null-indicators for y fields] [actual value for the first field] [actual value for the second field] ...
        //  1) For y fields, there is n-byte-null-indicators in the beginning of each record.
        //     The value n can be calculated as: ceil(y / 8). (e.g., 5 fields => ceil(5 / 8) = 1. 12 fields => ceil(12 / 8) = 2.)
        //     Each bit represents whether each field value is null or not.
        //     If k-th bit from the left is set to 1, k-th field value is null. We do not include anything in the actual data part.
        //     If k-th bit from the left is set to 0, k-th field contains non-null values.
        //     If there are more than 8 fields, then you need to find the corresponding byte first,
        //     then find a corresponding bit inside that byte.
        //  2) Actual data is a concatenation of values of the attributes.
        //  3) For Int and Real: use 4 bytes to store the value;
        //     For Varchar: use 4 bytes to store the length of characters, then store the actual characters.
        //  !!! The same format is used for updateRecord(), the returned data of readRecord(), and readAttribute().
        // For example, refer to the Q8 of Project 1 wiki page.

        // Insert a record into a file
        RC insertRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const void *data,
                        RID &rid);

        // Read a record identified by the given rid.
        RC
        readRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const RID &rid, void *data);

        // Print the record that is passed to this utility method.
        // This method will be mainly used for debugging/testing.
        // The format is as follows:
        // field1-name: field1-value  field2-name: field2-value ... \n
        // (e.g., age: 24  height: 6.1  salary: 9000
        //        age: NULL  height: 7.5  salary: 7500)
        RC printRecord(const std::vector<Attribute> &recordDescriptor, const void *data, std::ostream &out);

        /*****************************************************************************************************
        * IMPORTANT, PLEASE READ: All methods below this comment (other than the constructor and destructor) *
        * are NOT required to be implemented for Project 1                                                   *
        *****************************************************************************************************/
        RC deleteCurrentRecord(char *pageBufferPtr, const RID &rid, int shiftAmount);

        // Delete a record identified by the given rid.
        RC deleteRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const RID &rid);

        // Assume the RID does not change after an update
        RC updateRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const void *data,
                        const RID &rid);

        RC readFinalAttribute(char * pageReadBuffer, const std::vector<Attribute> &recordDescriptor, const RID &rid,
                              const std::string &attributeName, void *data);

        // Read an attribute given its name and the rid.
        RC readAttribute(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const RID &rid,
                         const std::string &attributeName, void *data);

        RC getFinalRID(FileHandle &fileHandle,const char *pageReadBuffer, const std::vector<Attribute> &recordDescriptor,
                                               const RID &rid, RID &finalRID);

        // Scan returns an iterator to allow the caller to go through the results one by one.
        RC scan(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                const std::string &conditionAttribute,
                const CompOp compOp,                  // comparison type such as "<" and "="
                const void *value,                    // used in the comparison
                const std::vector<std::string> &attributeNames, // a list of projected attributes
                RBFM_ScanIterator &rbfm_ScanIterator);

        // get size of input record
        int getRecordSize(int numFields, const std::vector<Attribute> &recordDescriptor, int nullIndicatorSize,
                          const void *data);

        bool checkBitSet(int index, char *inputRecordBitMap);

        bool createBufferRecord(char *bufferRecordPointerArray, char *inputRecordBitMap,
                                const std::vector<Attribute> &recordDescriptor, int numFieldsInRecord,
                                int nullIndicatorSize, char *inputRecordData, char *bufferRecordDataArray);

        RC insertRecordInPage(int recordSize, char *pageReadBuffer, char *bufferRecordStart, bool isNew, RID &rid);

        RC readRecordBasedOnDataType(const std::vector<Attribute> &recordDescriptor, int numFieldsInRecord,
                                     char *nullIndicatorPtr, char *pointerArrayStart, char *recordStartPointer,
                                     int prevptr, char *dataItr);

        bool getBitValueInTombStoneByte(char byte, TombStoneBit bitNum);

        void setBitValueInTombStoneByte(char *byte, TombStoneBit bitNum);

        void resetBitValueInTombStoneByte(char *byte, TombStoneBit bitNum);

        RC populateFilteredList(RBFM_ScanIterator &rbfm_ScanIterator, const std::vector<Attribute> &recordDescriptor,
                                const std::vector<std::string> &attributeNames);

        void setFilterAttrIndex(int *index, const std::vector<Attribute> &recordDescriptor,
                                const std::string &conditionAttribute);

        LinkedListPosition getLinkedListNodeType(char recordTombStoneByte);

        void getTombStoneData(const char *pageReadBuffer, char *recordTombStoneByte, int slotNum);

        void setLinkedListNodeType(char *byte, LinkedListPosition nodeType);

        int getRecordSizeFromSlot(int slotNum, const char *pageReadBuffer);

        int getNewRecordSize(const std::vector<Attribute> recordDescriptor, const void *data);
        void convertInputDataToRecord(const std::vector<Attribute> &recordDescriptor,  const void *data, char* bufferRecordStart);
        RC populatePageMetaData(char *pageBufferPtr, const RID &rid, PageMetaData &pageMeta);
        RC shiftRecords(char *pageBufferPtr, const RID &rid, int shiftAmount);
        RC updateDataAndPointers(char *pageBufferPtr, int shiftAmount, int index );
        //void createNewPageRecord(FileHandle &fileHandle,char *pageReadBuffer,char*bufferRecordStart,int recordSize,RID &rid);
    protected:
        RecordBasedFileManager();                                                   // Prevent construction
        ~RecordBasedFileManager();                                                  // Prevent unwanted destruction
        RecordBasedFileManager(const RecordBasedFileManager &);                     // Prevent construction by copying
        RecordBasedFileManager &operator=(const RecordBasedFileManager &);          // Prevent assignment

    };

} // namespace PeterDB

#endif // _rbfm_h_
