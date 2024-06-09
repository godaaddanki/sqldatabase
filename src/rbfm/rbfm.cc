#include "src/include/rbfm.h"


namespace PeterDB {
    RecordBasedFileManager &RecordBasedFileManager::instance() {
        static RecordBasedFileManager _rbf_manager = RecordBasedFileManager();
        return _rbf_manager;
    }

    static PagedFileManager *pfmManagerPtr = &PagedFileManager::instance();

    RecordBasedFileManager::RecordBasedFileManager() = default;

    RecordBasedFileManager::~RecordBasedFileManager() = default;

    RecordBasedFileManager::RecordBasedFileManager(const RecordBasedFileManager &) = default;

    RecordBasedFileManager &RecordBasedFileManager::operator=(const RecordBasedFileManager &) = default;


    LinkedListPosition RecordBasedFileManager::getLinkedListNodeType(char recordTombStoneByte) {
        bool bit1 = getBitValueInTombStoneByte(recordTombStoneByte, LinkedListBit1);
        bool bit2 = getBitValueInTombStoneByte(recordTombStoneByte, LinkedListBit2);
        if (bit1) {
            if (bit2) {
                return LinkedListEnd;
            } else {
                return LinkedListStart;
            }
        } else {
            if (bit2) {
                return LinkedListMiddle;
            } else {
                return NotLinkedListNode;
            }
        }
    }

    void RecordBasedFileManager::setLinkedListNodeType(char *byte, LinkedListPosition nodeType) {

        switch (nodeType) {
            case NotLinkedListNode:
                resetBitValueInTombStoneByte(byte, LinkedListBit1);
                resetBitValueInTombStoneByte(byte, LinkedListBit2);
                break;
            case LinkedListStart:
                setBitValueInTombStoneByte(byte, LinkedListBit1);
                resetBitValueInTombStoneByte(byte, LinkedListBit2);
                break;
            case LinkedListMiddle:
                resetBitValueInTombStoneByte(byte, LinkedListBit1);
                setBitValueInTombStoneByte(byte, LinkedListBit2);
                break;
            case LinkedListEnd:
                setBitValueInTombStoneByte(byte, LinkedListBit1);
                setBitValueInTombStoneByte(byte, LinkedListBit2);
                break;
            default:
                resetBitValueInTombStoneByte(byte, LinkedListBit1);
                resetBitValueInTombStoneByte(byte, LinkedListBit2);
                break;
        }

    }

    bool RecordBasedFileManager::getBitValueInTombStoneByte(char byte, TombStoneBit bitNum) {
        return (byte >> bitNum) & 1;
    }


    void RecordBasedFileManager::setBitValueInTombStoneByte(char *byte, TombStoneBit bitNum) {
        (*byte) |= (1 << bitNum);
    }

    void RecordBasedFileManager::resetBitValueInTombStoneByte(char *byte, TombStoneBit bitNum) {
        (*byte) &= ~(1 << bitNum);
    }

    RC RecordBasedFileManager::createFile(const std::string &fileName) {
        return pfmManagerPtr->createFile(fileName);
    }

    RC RecordBasedFileManager::destroyFile(const std::string &fileName) {
        return pfmManagerPtr->destroyFile(fileName);
    }

    RC RecordBasedFileManager::openFile(const std::string &fileName, FileHandle &fileHandle) {
        return pfmManagerPtr->openFile(fileName, fileHandle);
    }

    RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
        return pfmManagerPtr->closeFile(fileHandle);
    }

    int RecordBasedFileManager::getRecordSize(int numFields, const std::vector<Attribute> &recordDescriptor,
                                              int nullIndicatorSize, const void *data) {
        int recordSize = 0;
        char *recordStart = (char *) data;
        int itr = nullIndicatorSize;
        for (int index = 0; index < numFields; index++) {
            if (!checkBitSet(index, (char *) data)) {

                if (recordDescriptor[index].type == TypeInt) {
                    recordSize += sizeof(int);
                    itr += sizeof(int);
                } else if (recordDescriptor[index].type == TypeReal) {
                    recordSize += sizeof(float);
                    itr += sizeof(float);
                } else if (recordDescriptor[index].type == TypeVarChar) {
                    int length = *((int *) (recordStart + itr));
                    itr += sizeof(int);
                    itr += length;
                    recordSize += length;
                }
            }
        }
        int totalRecordSize = sizeof(int) + nullIndicatorSize + numFields * sizeof(int) + recordSize;
        if (totalRecordSize <= sizeof(unsigned) + sizeof(unsigned short)) {
            totalRecordSize = sizeof(unsigned) + sizeof(unsigned short);
        }
        return totalRecordSize;
//        return (sizeof(int)+nullIndicatorSize+numFields*sizeof(int)+recordSize);
    }

    bool RecordBasedFileManager::checkBitSet(int index, char *inputRecordBitMap) {
        int reqByteNum = index / 8;
        int reminder = index % 8;
        char reqByte = *(inputRecordBitMap + reqByteNum);
        return (reqByte >> (7 - reminder)) & 1;
    }

    bool RecordBasedFileManager::createBufferRecord(char *bufferRecordPointerArray, char *inputRecordBitMap,
                                                    const std::vector<Attribute> &recordDescriptor,
                                                    int numFieldsInRecord, int nullIndicatorSize, char *inputRecordData,
                                                    char *bufferRecordDataArray) {
        //copy record values and populate pointers
        char *bufferRecordPointerArrayItr = bufferRecordPointerArray;
        char *bufferRecordValueItr = bufferRecordDataArray;
        char *inputRecordItr = inputRecordData;
        int previousIndex = sizeof(int) + nullIndicatorSize + numFieldsInRecord * sizeof(int) - 1;
        int presentIndex = previousIndex;
        for (int index = 0; index < numFieldsInRecord; index++) {

            if (checkBitSet(index, inputRecordBitMap)) {
                //memcpy(bufferRecordPointerArray+sizeof(int)*index, &previousIndex, sizeof(int));
                memcpy(bufferRecordPointerArrayItr, &previousIndex, sizeof(int));
                bufferRecordPointerArrayItr += sizeof(int);
                continue;
            }

            if (recordDescriptor[index].type == TypeInt) {
                presentIndex += sizeof(int);
                memcpy(bufferRecordValueItr, inputRecordItr, sizeof(int));
                inputRecordItr += sizeof(int);
                bufferRecordValueItr += sizeof(int);

            } else if (recordDescriptor[index].type == TypeReal) {
                presentIndex += sizeof(float);
                memcpy(bufferRecordValueItr, inputRecordItr, sizeof(float));
                inputRecordItr += sizeof(float);
                bufferRecordValueItr += sizeof(float);

            } else if (recordDescriptor[index].type == TypeVarChar) {
                int varCharLength = 0;
                memcpy(&varCharLength, inputRecordItr, sizeof(int));
                presentIndex += varCharLength;
                inputRecordItr += sizeof(int);
                memcpy(bufferRecordValueItr, inputRecordItr, varCharLength);
                inputRecordItr += varCharLength;
                bufferRecordValueItr += varCharLength;

            }
//        else {
////            perror("Unknown record type");
//        }
            memcpy(bufferRecordPointerArrayItr, &presentIndex, sizeof(int));
            previousIndex = presentIndex;
            bufferRecordPointerArrayItr += sizeof(int);
        }
        return true;
    }

/*
void createNewPageRecord(FileHandle &fileHandle,char *pageReadBuffer,char*bufferRecordStart,int recordSize,  RID &rid){

}*/
    RC RecordBasedFileManager::insertRecordInPage(int recordSize, char *pageReadBuffer, char *bufferRecordStart,
                                                  bool isNew, RID &rid) {
        int freeSize = isNew ? PAGE_SIZE - 2 * sizeof(int) : *((int *) (pageReadBuffer + PAGE_SIZE - sizeof(int)));
        int currentFreeSpace = freeSize - recordSize - 2 * sizeof(int) - sizeof(char);

//    currentFreeSpace = 0;
        char *freeSpaceStart = pageReadBuffer + PAGE_SIZE - sizeof(int);
        memcpy(freeSpaceStart, &currentFreeSpace, sizeof(int));

        //Num Slots
        int presentSlots = *((int *) (pageReadBuffer + PAGE_SIZE - 2 * sizeof(int)));
        //Pointer
        int pointerValue = 0;
        if (!isNew) {
            pointerValue = PAGE_SIZE - 2 * sizeof(int) - presentSlots * (2 * sizeof(int) + sizeof(char)) - freeSize;
        }
        //TODO:: iterate through slots to find if there is a tombstone and update it
        int slotNumberForNewRecord;
        if (isNew) {
            slotNumberForNewRecord = 1;
        } else {
            //iterate through the slots to find if there is a tombstone slot
            int slotNum;
            for (slotNum = 1; slotNum <= presentSlots; slotNum++) {
                //get deleted node is available
                char recordTombStoneByte = 0;
                getTombStoneData(pageReadBuffer, &recordTombStoneByte, slotNum);
                // if record is already deleted , return error
                bool isNodeDeleted = getBitValueInTombStoneByte(recordTombStoneByte, NodeDeletionIndicator);
                if (isNodeDeleted) {
                    slotNumberForNewRecord = slotNum;
                    break;
                }
            }
            if (slotNum > presentSlots) {
                presentSlots += 1;
                slotNumberForNewRecord = presentSlots;
            }

        }
        //int numSlots = isNew ? 1 : presentSlots + 1;
        int totalSlots = isNew ? 1 : presentSlots;
        rid.slotNum = slotNumberForNewRecord;
        char *numSlotsStart = freeSpaceStart - sizeof(int);
        memcpy(numSlotsStart, &totalSlots, sizeof(int));
        //printf("freespace %d, slotnum%d , present slot%d", freeSize, slotNumberForNewRecord, totalSlots);

        char *tombStart = numSlotsStart - rid.slotNum * (2 * sizeof(int) + sizeof(char));
        char *pointerStart = tombStart + sizeof(char);
        char *sizeRecordStart = pointerStart + sizeof(int);
        char tmp = 0;
        memcpy(tombStart, &tmp, sizeof(char));
        memcpy(pointerStart, &pointerValue, sizeof(int));
        memcpy(sizeRecordStart, &recordSize, sizeof(int));

        //Copy record
        memcpy(pageReadBuffer + pointerValue, bufferRecordStart, recordSize);

        return PASS;

    }

    RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const void *data, RID &rid) {
        //sizes
        int numFieldsInRecord = recordDescriptor.size();
        int nullIndicatorSize = ceil((float) numFieldsInRecord / 8);
        int recordSize = getRecordSize(numFieldsInRecord, recordDescriptor, nullIndicatorSize, data);

        //tmp buffer record pointers
        char *bufferRecordStart = (char *) malloc(recordSize);
        char *bufferRecordBitMap = bufferRecordStart + sizeof(int);
        char *bufferRecordPointerArray = bufferRecordStart + sizeof(int) + nullIndicatorSize;
        char *bufferRecordDataArray = bufferRecordPointerArray + numFieldsInRecord * sizeof(int);

        //input record pointers
        char *inputRecordStart = (char *) data;
        char *inputRecordBitMap = inputRecordStart;
        char *inputRecordData = inputRecordStart + nullIndicatorSize;

        //temp buffer for page
        char *pageReadBuffer = (char *) malloc(PAGE_SIZE * sizeof(char));

        //copy bit map and num records
        memcpy(bufferRecordBitMap, inputRecordBitMap, nullIndicatorSize);
        memcpy(bufferRecordStart, &numFieldsInRecord, sizeof(int));

        //Copy data to buffer record
        createBufferRecord(bufferRecordPointerArray, inputRecordBitMap, recordDescriptor, numFieldsInRecord,
                           nullIndicatorSize, inputRecordData, bufferRecordDataArray);

        // select a page slot for record
        PageNum pageCount = fileHandle.getNumberOfPages();
        if (pageCount == 0) {
            // create new page with new record
            //createNewPageRecord(fileHandle,pageReadBuffer,bufferRecordStart,recordSize,rid);
            bool isNew = true;
            insertRecordInPage(recordSize, pageReadBuffer, bufferRecordStart, isNew, rid);
            //Append page to file
            fileHandle.appendPage(pageReadBuffer);
            rid.pageNum = 0;
        }
//    else if (pageCount<0) {
////        perror("File handle not valid");
//    }
        else {
            // check free space available in latest page
            fileHandle.readPage(pageCount - 1, pageReadBuffer);

            //check free space
            int existingFreeSpace = *((int *) (pageReadBuffer + PAGE_SIZE - sizeof(int)));
            int freeSlotFound = false;
            if (existingFreeSpace > (recordSize + 2 * sizeof(int))) {
                bool isNew = false;
                insertRecordInPage(recordSize, pageReadBuffer, bufferRecordStart, isNew, rid);
                rid.pageNum = pageCount - 1;
                freeSlotFound = true;
                fileHandle.writePage(rid.pageNum, pageReadBuffer);
            } else {

                for (int i = 0; i < pageCount - 1; i++) {
                    // check free space available in the latest page
                    fileHandle.readPage(i, pageReadBuffer);

                    //check free space
                    int existingFreeSpace = *((int *) (pageReadBuffer + PAGE_SIZE - sizeof(int)));
                    if (existingFreeSpace > (recordSize + 2 * sizeof(int))) {
                        bool isNew = false;
                        insertRecordInPage(recordSize, pageReadBuffer, bufferRecordStart, isNew, rid);
                        rid.pageNum = i;
                        freeSlotFound = true;
                        fileHandle.writePage(rid.pageNum, pageReadBuffer);
                        break;
                    }
                }
            }
            if (!freeSlotFound) {
                bool isNew = true;
                insertRecordInPage(recordSize, pageReadBuffer, bufferRecordStart, isNew, rid);
                //Append page to file
                fileHandle.appendPage(pageReadBuffer);
                rid.pageNum = pageCount;
            }
        }
        //printf(" record inserted , record size %d,RID pagenum %d,RID slotnum %d", recordSize, rid.pageNum, rid.slotNum);
        free(bufferRecordStart);
        free(pageReadBuffer);
        bufferRecordStart = NULL;
        pageReadBuffer = NULL;
//    std::cout<<"255"<<std::endl;

        return PASS;
    }

    RC RecordBasedFileManager::readRecordBasedOnDataType(const std::vector<Attribute> &recordDescriptor,
                                                         int numFieldsInRecord, char *nullIndicatorPtr,
                                                         char *pointerArrayStart, char *recordStartPointer, int prevptr,
                                                         char *dataItr) {
        for (int index = 0; index < numFieldsInRecord; index++) {
            if (!checkBitSet(index, nullIndicatorPtr)) {
                char *ptrPtr = pointerArrayStart + index * sizeof(int);
                int ptrValue = *((int *) ptrPtr);
//                std::cout<<"pointer_value"<<ptrValue<<std::endl;
                char *valuePtr = recordStartPointer + prevptr + 1;
                int length = ptrValue - prevptr;
                if (recordDescriptor[index].type == TypeInt) {
//                    std::cout<<"292"<<std::endl;
                    memcpy(dataItr, valuePtr, sizeof(int));
                    dataItr += sizeof(int);
                } else if (recordDescriptor[index].type == TypeVarChar) {
//                    std::cout<<"302";
                    memcpy(dataItr, &length, sizeof(int));
                    dataItr += sizeof(int);
                    memcpy(dataItr, valuePtr, length);
                    dataItr += length;
                } else if (recordDescriptor[index].type == TypeReal) {
//                    std::cout<<"297";
                    memcpy(dataItr, valuePtr, sizeof(float));
                    dataItr += sizeof(float);
                }
//                else {
//                 perror("Unknown record type");
//                }
                prevptr = ptrValue;
            }
        }
        return PASS;
    }

    RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                          const RID &rid, void *data) {
//    std::cout<<"inside read";
        char *dataItr = (char *) data;
        char *pageBufferPtr = (char *) malloc(sizeof(char) * PAGE_SIZE);
        // check if page exists
        if (fileHandle.readPage(rid.pageNum, pageBufferPtr) == -1) {
            free(pageBufferPtr);
            pageBufferPtr = NULL;
            return FAILED;
        }

        //  find the final RID for the readpage, check if its a deleted record or if its a tombstone
        RID finalRID;
        if (getFinalRID(fileHandle, pageBufferPtr, recordDescriptor, rid, finalRID) == -1) {
            free(pageBufferPtr);
            pageBufferPtr = NULL;
            return -1;
        }

        // if RID changed, go to the new page to get the new record
        if (rid.pageNum != finalRID.pageNum) {
            if (fileHandle.readPage(finalRID.pageNum, pageBufferPtr) == -1) {
                free(pageBufferPtr);
                pageBufferPtr = NULL;
                return FAILED;
            }
        }

        // Calculating index of the bytes location
        char *recordMetaStart =
                pageBufferPtr + PAGE_SIZE - 2 * sizeof(int) - finalRID.slotNum * (2 * sizeof(int) + sizeof(char));
//        char* recordSizeStart = recordMetaStart+sizeof(int);
//        int indexRecordSize = *((int*) recordSizeStart);
        char *tombStartLocation = recordMetaStart;
        int pointerLocation = *((int *) (tombStartLocation + sizeof(char)));
        char *recordStartPointer = pageBufferPtr + pointerLocation;

        int numFieldsInRecord = *((int *) recordStartPointer);
        int nullIndicatorSize = ceil((float) numFieldsInRecord / 8);
        char *nullIndicatorPtr = recordStartPointer + sizeof(int);
        char *pointerArrayStart = nullIndicatorPtr + nullIndicatorSize;


//        char* valuePtrStart = pointerArrayStart+sizeof(int)*numFieldsInRecord;
        //copying NULL bitmap
        memcpy(dataItr, nullIndicatorPtr, nullIndicatorSize);
        dataItr += nullIndicatorSize;

        //copying data
        int prevptr = sizeof(int) + nullIndicatorSize + sizeof(int) * numFieldsInRecord - 1;
//        std::cout<<"start pointer_value"<<prevptr<<std::endl;
//        std::cout<<"*** new iteration**"<<numFieldsInRecord<<std::endl;
        readRecordBasedOnDataType(recordDescriptor, numFieldsInRecord, nullIndicatorPtr, pointerArrayStart,
                                  recordStartPointer, prevptr, dataItr);
        free(pageBufferPtr);
        pageBufferPtr = NULL;
        return PASS;

    }


    RC RecordBasedFileManager::printRecord(const std::vector<Attribute> &recordDescriptor, const void *data,
                                           std::ostream &out) {

        //<AttributeName1>:\s<Value1>,\s<AttributeName2>:\s<Value2>,\s<AttributeName3>:\s<Value3>\n

        char *inputRecordStart = (char *) data;
        char *inputRecordBitMap = inputRecordStart;

        int numFieldsInRecord = recordDescriptor.size();
        int nullIndicatorSize = ceil((float) numFieldsInRecord / 8);

        char *inputRecordData = inputRecordStart + nullIndicatorSize;
       // printf("numfelds %d , null size %d \n", numFieldsInRecord, nullIndicatorSize);
        for (int index = 0; index < numFieldsInRecord; index++) {
            if (checkBitSet(index, inputRecordBitMap)) {
                out << recordDescriptor[index].name << ": NULL";
             //   std::cout << recordDescriptor[index].name << ": NULL";
            } else {
                if (recordDescriptor[index].type == TypeInt) {
                    int fieldValue;
                    memcpy(&fieldValue, inputRecordData, sizeof(int));
                    out << recordDescriptor[index].name.c_str() << ": " << fieldValue;
                 //   std::cout << recordDescriptor[index].name << ": " << fieldValue;
                    inputRecordData += sizeof(int);
                } else if (recordDescriptor[index].type == TypeReal) {
                    float fieldValue;
                    memcpy(&fieldValue, inputRecordData, sizeof(float));
                    out << recordDescriptor[index].name.c_str() << ": " << fieldValue;
                //    std::cout << recordDescriptor[index].name << ": " << fieldValue;
                    inputRecordData += sizeof(float);
                } else if (recordDescriptor[index].type == TypeVarChar) {
                    int varSize = 0;
                    memcpy(&varSize, inputRecordData, sizeof(int));
                    //printf("%d\n", varSize);
                    inputRecordData += sizeof(int);
                    char *fieldValue = (char *) malloc(varSize + 1);
                    fieldValue[varSize] = '\0';
                    memcpy(fieldValue, inputRecordData, varSize);
                    out << recordDescriptor[index].name.c_str() << ": " << fieldValue;
                 //   std::cout << recordDescriptor[index].name << ": " << std::string(fieldValue);
                    inputRecordData += varSize;
                    free(fieldValue);
                    fieldValue = NULL;
                }
//            else {
//               // perror("Unknown record type");
//            }
            }
            if (index != numFieldsInRecord - 1) {
                out << ", ";
             //   std::cout << ", ";
            }
        }

        out << "\n";
     //   std::cout << "\n";
        return PASS;

    }

    RC RecordBasedFileManager::populatePageMetaData(char *pageBufferPtr, const RID &rid, PageMetaData &pageMeta) {

        // check if page exists
        pageMeta.freeSpace = *((int *) (pageBufferPtr + PAGE_SIZE - sizeof(int)));
        pageMeta.numSlotsUsed = *((int *) (pageBufferPtr + PAGE_SIZE - 2 * sizeof(int)));
        // Calculating index of the bytes location
        char *recordMetaStart =
                pageBufferPtr + PAGE_SIZE - 2 * sizeof(int) - rid.slotNum * (2 * sizeof(int) + sizeof(char));
        char *tombStartLocation = recordMetaStart;
        pageMeta.tombStoneByte = *tombStartLocation;
        int pointerLocation = *((int *) (tombStartLocation + sizeof(char)));
        pageMeta.recordStartOffset = pointerLocation;
        char *recordLengthStart = tombStartLocation + sizeof(char) + sizeof(int);
        pageMeta.recordSize = *((int *) recordLengthStart);
        return PASS;
    }

    RC RecordBasedFileManager::updateDataAndPointers(char *pageBufferPtr, int shiftAmount, int index) {
        char *recordMetaStart = pageBufferPtr + PAGE_SIZE - 2 * sizeof(int) - index * (2 * sizeof(int) + sizeof(char));
        char *tombStartLocation = recordMetaStart;
        char *pointerStart = tombStartLocation + sizeof(char);
        int pointerLocation = *((int *) (pointerStart));
        char *recordLengthStart = tombStartLocation + sizeof(char) + sizeof(int);
        int recordSize = *((int *) recordLengthStart);
        char *recordPointer = pageBufferPtr + pointerLocation;
        if(pointerLocation+shiftAmount < 0)
        {
            std::cout<<"leaving the page boundary"<<std::endl;
            std::cout<<"record start is at"<<pointerLocation<<std::endl;
            std::cout<<"planning to shift record by"<<shiftAmount<<std::endl;
        }
        if (recordSize) {
            char *temp = (char *) malloc(recordSize);
            memcpy(temp, recordPointer, recordSize);
            memcpy(recordPointer + shiftAmount, temp, recordSize);
            free(temp);
            temp  = NULL;
            *((int *) pointerStart) += shiftAmount;
        }
        return PASS;
    }

    RC RecordBasedFileManager::shiftRecords(char *pageBufferPtr, const RID &rid, int shiftAmount) {
        //Always call shift before insertion

        PageMetaData pageMeta;
        populatePageMetaData(pageBufferPtr, rid, pageMeta);
        if(shiftAmount==0){
            return PASS;
        }

        if (shiftAmount > 0) {
            //Bigger record insertion(make room from last)
            for (int index = pageMeta.numSlotsUsed; index >= rid.slotNum + 1; index--) {
                updateDataAndPointers(pageBufferPtr, shiftAmount, index);
            }
        } else {
            //Smaller record insertion(start squeezing in from start)
            for (int index = rid.slotNum + 1; index <= pageMeta.numSlotsUsed; index++) {
                updateDataAndPointers(pageBufferPtr, shiftAmount, index);
            }
        }
        //update free space
        char *freeSpacePointer = pageBufferPtr + PAGE_SIZE - sizeof(int);
        int newFreeSpace = *((int *) freeSpacePointer) - shiftAmount;
        *((int *) freeSpacePointer) = newFreeSpace;

        return PASS;
    }

    RC RecordBasedFileManager::deleteCurrentRecord(char *pageBufferPtr, const RID &rid, int shiftAmount) {
//        std::cout<<"trying to delete  record with page : "<<rid.pageNum<<"slot : "<<rid.slotNum<<std::endl;
        //get slot information
     //   printf("deleting current record");
        char *recordMetaStart =
                pageBufferPtr + PAGE_SIZE - 2 * sizeof(int) - rid.slotNum * (2 * sizeof(int) + sizeof(char));
        char *tombStartLocation = recordMetaStart;
        char *pointerLocationStart = tombStartLocation + sizeof(char);
        char *recordSizeStart = pointerLocationStart + sizeof(int);

        // set tombstone byte about delete record and set slot values to 0
        char recordTombStoneByte = 0;
        getTombStoneData(pageBufferPtr, &recordTombStoneByte, rid.slotNum);
        setBitValueInTombStoneByte(&recordTombStoneByte, NodeDeletionIndicator);

        // shift record to delete the current record
        shiftRecords(pageBufferPtr, rid, shiftAmount);

        *tombStartLocation = recordTombStoneByte;
        *((int*)pointerLocationStart) = 0;
        *((int*)recordSizeStart) = 0;

        return 0;
    }

    RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const RID &rid) {
        // read the page
        //printf("entered delete record, RID pagenum %d,RID slotnum %d", rid.pageNum, rid.slotNum);
        char *pageBufferPtr = (char *) malloc(sizeof(char) * PAGE_SIZE);
        // check if page exists
        if (fileHandle.readPage(rid.pageNum, pageBufferPtr) == -1) {
            free(pageBufferPtr);
            pageBufferPtr = NULL;
            return FAILED;
        }

        //get tomb stone data for this record
        char recordTombStoneByte = 0;
        getTombStoneData(pageBufferPtr, &recordTombStoneByte, rid.slotNum);

        // if record is already deleted , return error
        bool isNodeDeleted = getBitValueInTombStoneByte(recordTombStoneByte, NodeDeletionIndicator);
        if (isNodeDeleted) {
            free(pageBufferPtr);
            pageBufferPtr = NULL;
            return FAILED;
        }

        //if RID is a tombstone, find next RID location and delete the current node
        bool isRecordTombStone = getBitValueInTombStoneByte(recordTombStoneByte, TombStone);

        if (isRecordTombStone) {

            LinkedListPosition tombStoneNodeType = getLinkedListNodeType(recordTombStoneByte);
            //invalid case should not happen
            if (tombStoneNodeType == NotLinkedListNode) {
                return FAILED;
            }

            RID presentRID;
            presentRID.slotNum = rid.slotNum;
            presentRID.pageNum = rid.pageNum;
            char presentRecordTombStoneByte = 0;
            LinkedListPosition presentTombStoneNodeType;
            char *presentPageReadBuffer = (char *) malloc(PAGE_SIZE);
//            printf(" pointer initial value %p \n",presentPageReadBuffer);
            //TODO::page read optimization
            do {
                //For present node

//                std::cout<<"pointer value while allocation"<<std::hex<<presentPageReadBuffer<<std::endl;

                if (fileHandle.readPage(presentRID.pageNum, presentPageReadBuffer) == -1) {
                    free(presentPageReadBuffer);
                    presentPageReadBuffer = NULL;
                    return -1;
                }
                getTombStoneData(presentPageReadBuffer, &presentRecordTombStoneByte, presentRID.slotNum);
                presentTombStoneNodeType = getLinkedListNodeType(presentRecordTombStoneByte);

                //filter invalid cases like deleted nodes
                bool isNodeDeleted = getBitValueInTombStoneByte(presentRecordTombStoneByte, NodeDeletionIndicator);
                if (isNodeDeleted) {
                    // printf("node is already deleted");
                    free(presentPageReadBuffer);
                    presentPageReadBuffer = NULL;
                    return PASS;
                }

                const char *recordMetaStart =
                        presentPageReadBuffer + PAGE_SIZE - 2 * sizeof(int) -
                        presentRID.slotNum * (2 * sizeof(int) + sizeof(char));
                const char *tombStartLocation = recordMetaStart;
                const char *pointerLocationStart = tombStartLocation + sizeof(char);
                int pointerLocation = *((int *) (pointerLocationStart));
                const char *recordStartPointer = presentPageReadBuffer + pointerLocation;

                RID tmpRID;
                if (presentTombStoneNodeType != LinkedListEnd) {
                    tmpRID.pageNum = *((unsigned *) recordStartPointer);
                    tmpRID.slotNum = *((unsigned short *) (recordStartPointer + sizeof(unsigned)));
                }
                int shiftAmount;
                //delete the current node and shift by size of the record
//                if (presentTombStoneNodeType == LinkedListMiddle || presentTombStoneNodeType == LinkedListStart) {
//                    shiftAmount = sizeof(unsigned)+sizeof(unsigned short);
//                } else {
                    PageMetaData pageMeta;
                    populatePageMetaData(presentPageReadBuffer, presentRID,pageMeta);
                     shiftAmount = pageMeta.recordSize;
//                }

                shiftAmount = -1 * shiftAmount;
                //printf(" reached middle or start  of the tombstone , shiftAmount %d,RID pagenum %d,RID slotnum %d",
                      // shiftAmount, presentRID.pageNum, presentRID.slotNum);
                deleteCurrentRecord(presentPageReadBuffer, presentRID, shiftAmount);
//                printf(" pointer initial value 2 %p \n",presentPageReadBuffer);
                if (fileHandle.writePage(presentRID.pageNum, presentPageReadBuffer) == -1) {
                    free(presentPageReadBuffer);
                    presentPageReadBuffer = NULL;
                    return -1;
                }
                presentRID.pageNum = tmpRID.pageNum;
                presentRID.slotNum = tmpRID.slotNum;



            } while (presentTombStoneNodeType != LinkedListEnd);
//            printf(" pointer initial value 3 %p  \n",presentPageReadBuffer);
            free(presentPageReadBuffer);
            presentPageReadBuffer = NULL;
        } else {
            const char *recordMetaStart =
                    pageBufferPtr + PAGE_SIZE - 2 * sizeof(int) -
                    rid.slotNum * (2 * sizeof(int) + sizeof(char));
            const char *tombStartLocation = recordMetaStart;
            const char *pointerLocationStart = tombStartLocation + sizeof(char);
            int pointerLocation = *((int *) (pointerLocationStart));
            const char *recordStartPointer = pageBufferPtr + pointerLocation;
            PageMetaData pageMeta;
            populatePageMetaData(pageBufferPtr, rid,pageMeta);
            int shiftAmount = pageMeta.recordSize;
            shiftAmount = -1 * shiftAmount;
//            printf(" not a tombstone , shiftAmount %d,RID pagenum %d,RID slotnum %d", shiftAmount, rid.pageNum,
//             rid.slotNum);
            deleteCurrentRecord(pageBufferPtr, rid, shiftAmount);
            if (fileHandle.writePage(rid.pageNum, pageBufferPtr) == -1) {
                free(pageBufferPtr);
                pageBufferPtr = NULL;
                return -1;
            }
        }
        free(pageBufferPtr);
        pageBufferPtr = NULL;
        return 0;

    }

    void RecordBasedFileManager::getTombStoneData(const char *pageReadBuffer, char *recordTombStoneByte, int slotNum) {
        const char *recordMetaStart =
                pageReadBuffer + PAGE_SIZE - 2 * sizeof(int) - slotNum * (2 * sizeof(int) + sizeof(char));
        *recordTombStoneByte = *recordMetaStart;
    }

    int RecordBasedFileManager::getRecordSizeFromSlot(int slotNum, const char *pageReadBuffer) {
        const char *recordMetaStart =
                pageReadBuffer + PAGE_SIZE - 2 * sizeof(int) - slotNum * (2 * sizeof(int) + sizeof(char));
        const char *recordSizeStart = recordMetaStart + sizeof(char) + sizeof(int);
        return *((int *) recordSizeStart);
    }

    int RecordBasedFileManager::getNewRecordSize(const std::vector<Attribute> recordDescriptor, const void *data) {
        int numFieldsInRecord = recordDescriptor.size();
        int nullIndicatorSize = ceil((float) numFieldsInRecord / 8);
        int recordSize = getRecordSize(numFieldsInRecord, recordDescriptor, nullIndicatorSize, data);
        return recordSize;
    }

    RC RecordBasedFileManager::getFinalRID(FileHandle &fileHandle, const char *pageReadBuffer,
                                           const std::vector<Attribute> &recordDescriptor, const RID &rid,
                                           RID &finalRID) {
        // printf("entered get final RID");
        //get tomb stone data for this record
        char recordTombStoneByte = 0;
        getTombStoneData(pageReadBuffer, &recordTombStoneByte, rid.slotNum);
//        std::cout<<"tomb stone byte is"<<recordTombStoneByte<<std::endl;

        //filter invalid cases like deleted nodes and linked list middle nodes
        bool isNodeDeleted = getBitValueInTombStoneByte(recordTombStoneByte, NodeDeletionIndicator);
        if (isNodeDeleted) {
            //printf("node is already deleted");
            return FAILED;
        }

        //if RID is a tombstone, go to the next RID location
        bool isRecordTombStone = getBitValueInTombStoneByte(recordTombStoneByte, TombStone);

        if (isRecordTombStone) {
            LinkedListPosition tombStoneNodeType = getLinkedListNodeType(recordTombStoneByte);
            //invalid case should not happen
            if (tombStoneNodeType == NotLinkedListNode) {
                finalRID.pageNum = rid.pageNum;
                finalRID.slotNum = rid.slotNum;
                return PASS;
            }

            RID presentRID;
            presentRID.slotNum = rid.slotNum;
            presentRID.pageNum = rid.pageNum;
            char presentRecordTombStoneByte = 0;
            LinkedListPosition presentTombStoneNodeType;
            do {
                //For present node
                char *presentPageReadBuffer = (char *) malloc(PAGE_SIZE);
                if (fileHandle.readPage(presentRID.pageNum, presentPageReadBuffer) == -1) {
                    return -1;
                }
                getTombStoneData(presentPageReadBuffer, &presentRecordTombStoneByte, presentRID.slotNum);
                presentTombStoneNodeType = getLinkedListNodeType(presentRecordTombStoneByte);

                //filter invalid cases like deleted nodes
                bool isNodeDeleted = getBitValueInTombStoneByte(presentRecordTombStoneByte, NodeDeletionIndicator);
                if (isNodeDeleted) {
                //    printf("node is already deleted");
                    free(presentPageReadBuffer);
                    presentPageReadBuffer = NULL;
                    return FAILED;
                }

                //get next RID
                const char *recordMetaStart =
                        presentPageReadBuffer + PAGE_SIZE - 2 * sizeof(int) -
                        presentRID.slotNum * (2 * sizeof(int) + sizeof(char));
                const char *tombStartLocation = recordMetaStart;
                const char *pointerLocationStart = tombStartLocation + sizeof(char);
                int pointerLocation = *((int *) (pointerLocationStart));
                const char *recordStartPointer = presentPageReadBuffer + pointerLocation;
                if (presentTombStoneNodeType != LinkedListEnd) {
                    presentRID.pageNum = *((unsigned *) recordStartPointer);
                    presentRID.slotNum = *((unsigned short *) (recordStartPointer + sizeof(unsigned)));
                }
                free(presentPageReadBuffer);
                presentPageReadBuffer = NULL;

            } while (presentTombStoneNodeType != LinkedListEnd);
            //at linked list end
            finalRID.pageNum = presentRID.pageNum;
            finalRID.slotNum = presentRID.slotNum;

        } else {
            finalRID.pageNum = rid.pageNum;
            finalRID.slotNum = rid.slotNum;
        }
        //std::cout << "final rid" << finalRID.pageNum << finalRID.slotNum;
        return 0;
    }

    void
    RecordBasedFileManager::convertInputDataToRecord(const std::vector<Attribute> &recordDescriptor, const void *data,
                                                     char *bufferRecordStart) {
        //sizes
        int numFieldsInRecord = recordDescriptor.size();
        int nullIndicatorSize = ceil((float) numFieldsInRecord / 8);
        int recordSize = getRecordSize(numFieldsInRecord, recordDescriptor, nullIndicatorSize, data);

        char *bufferRecordBitMap = bufferRecordStart + sizeof(int);
        char *bufferRecordPointerArray = bufferRecordStart + sizeof(int) + nullIndicatorSize;
        char *bufferRecordDataArray = bufferRecordPointerArray + numFieldsInRecord * sizeof(int);

        //input record pointers
        char *inputRecordStart = (char *) data;
        char *inputRecordBitMap = inputRecordStart;
        char *inputRecordData = inputRecordStart + nullIndicatorSize;


        //copy bit map and num records
        memcpy(bufferRecordBitMap, inputRecordBitMap, nullIndicatorSize);
        memcpy(bufferRecordStart, &numFieldsInRecord, sizeof(int));

        //Copy data to buffer record
        createBufferRecord(bufferRecordPointerArray, inputRecordBitMap, recordDescriptor, numFieldsInRecord,
                           nullIndicatorSize, inputRecordData, bufferRecordDataArray);
    }

    RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const void *data, const RID &rid) {
        //get page
        char *pageReadBuffer = (char *) malloc(PAGE_SIZE * sizeof(char));
        int RC = fileHandle.readPage(rid.pageNum, pageReadBuffer);
        //std::ostringstream out;
        //printRecord(recordDescriptor, pageReadBuffer, out);
        if (RC == -1) {
            free(pageReadBuffer);
            pageReadBuffer = NULL;
            return FAILED;
        }

        //Get meta data of the page
        PageMetaData pageMeta;
        populatePageMetaData(pageReadBuffer, rid, pageMeta);

        //get tomb stone data for this record
        char recordTombStoneByte = 0;
        getTombStoneData(pageReadBuffer, &recordTombStoneByte, rid.slotNum);

        //filter invalid cases like deleted nodes and linked list middle nodes
        bool isNodeDeleted = getBitValueInTombStoneByte(recordTombStoneByte, NodeDeletionIndicator);
        if (isNodeDeleted) {
            free(pageReadBuffer);
            pageReadBuffer = NULL;
            return FAILED;
        }

        //get existing record size
        int storedRecordSize = getRecordSizeFromSlot(rid.slotNum, pageReadBuffer);
//        void *storedRecord = malloc(storedRecordSize);
//
//        //get  record size
//        readRecord(fileHandle, recordDescriptor, rid, storedRecord);

        //get new record size
        int newRecordSize = getNewRecordSize(recordDescriptor, data);

        bool isPresentRecordTombStone = getBitValueInTombStoneByte(recordTombStoneByte, TombStone);
        RID finalRID;
        getFinalRID(fileHandle, pageReadBuffer, recordDescriptor, rid, finalRID);
        char *recordPointer = pageReadBuffer + pageMeta.recordStartOffset;
        if (newRecordSize < storedRecordSize) {
            //if req size less
            int shiftAmount = newRecordSize - storedRecordSize;
            shiftRecords(pageReadBuffer, finalRID, shiftAmount);

            //update rec size in slot
            char *tombStoneStart =
                    pageReadBuffer + PAGE_SIZE - 2 * sizeof(int) - rid.slotNum * (2 * sizeof(int) + sizeof(char));
            char *recordSizePtr = tombStoneStart + sizeof(char) + sizeof(int);
            *((int *) recordSizePtr) = newRecordSize;

            //convert input data to record format
            //tmp buffer record pointers
            char *bufferRecordStart = (char *) malloc(newRecordSize);
            convertInputDataToRecord(recordDescriptor, data, bufferRecordStart);

            //write record
            memcpy(recordPointer, bufferRecordStart, newRecordSize);
            free(bufferRecordStart);
            bufferRecordStart = NULL;

        } else if (newRecordSize > storedRecordSize) {
            //req size greater
            if (pageMeta.freeSpace >= newRecordSize - storedRecordSize) {
                //Free available in current page
                int shiftAmount = newRecordSize - storedRecordSize;
                shiftRecords(pageReadBuffer, finalRID, shiftAmount);

                //update rec size in slot
                char *tombStoneStart =
                        pageReadBuffer + PAGE_SIZE - 2 * sizeof(int) - rid.slotNum * (2 * sizeof(int) + sizeof(char));
                char *recordSizePtr = tombStoneStart + sizeof(char) + sizeof(int);
                *((int *) recordSizePtr) = newRecordSize;

                //convert input data to record format
                //tmp buffer record pointers
                char *bufferRecordStart = (char *) malloc(newRecordSize);
                convertInputDataToRecord(recordDescriptor, data, bufferRecordStart);

                //write record
                memcpy(recordPointer, bufferRecordStart, newRecordSize);
                free(bufferRecordStart);
                bufferRecordStart = NULL;
            } else {
                //Tomb stone creation
                int shiftAmount = sizeof(unsigned) + sizeof(unsigned short) - storedRecordSize;
                shiftRecords(pageReadBuffer, finalRID, shiftAmount);

                //insert record
                RID newRID;
                insertRecord(fileHandle, recordDescriptor, data, newRID);

                //copy rid to old record data
                char *recordPtr = pageReadBuffer + pageMeta.recordStartOffset;
                char *pageNumPtr = recordPtr;
                char *pageSlotNumPtr = recordPtr + sizeof(unsigned);
                memcpy(pageNumPtr, &newRID.pageNum, sizeof(unsigned));
                memcpy(pageSlotNumPtr, &newRID.slotNum, sizeof(unsigned short));

                //update tomb stone data of old node.
                char byte = 0;
                if (isPresentRecordTombStone) { //  Middle node
                    //Set tombstone bit
                    setBitValueInTombStoneByte(&byte, TombStone);
                    //middle node
                    setLinkedListNodeType(&byte, LinkedListMiddle);
                } else {  //Start node
                    //Set tombstone bit
                    setBitValueInTombStoneByte(&byte, TombStone);
                    //middle node
                    setLinkedListNodeType(&byte, LinkedListStart);
                }

                //update tomb stone data of new node.
                char *pageReadBufferNew = (char *) malloc(PAGE_SIZE * sizeof(char));
                int RC = fileHandle.readPage(newRID.pageNum, pageReadBufferNew);
                if (RC == -1) {
                    free(pageReadBuffer);
                    free(pageReadBufferNew);
                    pageReadBuffer = NULL;
                    pageReadBufferNew = NULL;
                    return FAILED;
                }
                //Set tombstone bit
                char byteNew = 0;
                setBitValueInTombStoneByte(&byteNew, TombStone);
                //End node
                setLinkedListNodeType(&byteNew, LinkedListEnd);
                char *newTombStoneStart = pageReadBufferNew + PAGE_SIZE - 2 * sizeof(int) -
                                          newRID.slotNum * (2 * sizeof(int) + sizeof(char));
                *newTombStoneStart = byteNew;

                //update tomb stone byte and rec size in old slot.
                char *tombStoneStart =
                        pageReadBuffer + PAGE_SIZE - 2 * sizeof(int) - rid.slotNum * (2 * sizeof(int) + sizeof(char));
                char *recordSizePtr = tombStoneStart + sizeof(char) + sizeof(int);
                *tombStoneStart = byte;
                *((int *) recordSizePtr) = sizeof(unsigned) + sizeof(unsigned short);
                fileHandle.writePage(newRID.pageNum, pageReadBufferNew);
                free(pageReadBufferNew);
                pageReadBufferNew = NULL;

            }
        }
        fileHandle.writePage(rid.pageNum, pageReadBuffer);
        free(pageReadBuffer);
        pageReadBuffer = NULL;
        return PASS;
    }


    RC RecordBasedFileManager::readFinalAttribute(char *pageReadBuffer, const std::vector<Attribute> &recordDescriptor,
                                                  const RID &rid, const std::string &attributeName, void *data) {

        char *recordMetaStart =
                pageReadBuffer + PAGE_SIZE - 2 * sizeof(int) - rid.slotNum * (2 * sizeof(int) + sizeof(char));
        char *tombStartLocation = recordMetaStart;
        char *recordPtrPtr = tombStartLocation + sizeof(char);
        int pointerLocation = *((int *) (recordPtrPtr));
        char *recordStartPointer = pageReadBuffer + pointerLocation;

        int numFieldsInRecord = *((int *) recordStartPointer);
        int nullIndicatorSize = ceil((float) numFieldsInRecord / 8);
        char *nullIndicatorPtr = recordStartPointer + sizeof(int);
        char *pointerArrayStart = nullIndicatorPtr + nullIndicatorSize;

        char *dataItr = (char *) data;

        //get index of the required attribute
        int requiredIndex = -1;
        for (int index = 0; index < recordDescriptor.size(); index++) {
            if (recordDescriptor[index].name == attributeName) {
                requiredIndex = index;
                break;
            }
        }
        if (requiredIndex == -1) {
            return FAILED;
        }


        // shift pointer array start to required pointer location
        char *currentIndexPtr = pointerArrayStart + requiredIndex * sizeof(int);
        char *preIndexPtr = pointerArrayStart + (requiredIndex - 1) * sizeof(int);
        int previousIndexOffset;

        if (requiredIndex == 0) {
            previousIndexOffset = sizeof(int) + nullIndicatorSize + sizeof(int) * numFieldsInRecord - 1;
        } else {
            previousIndexOffset = (*((int *) preIndexPtr));
        }

        // point to required data location
        char *valuePtr = recordStartPointer + previousIndexOffset + 1;

        int requiredDataLength = *(int *) currentIndexPtr - previousIndexOffset;

        if (!checkBitSet(requiredIndex, nullIndicatorPtr)) {
            //first filed is NULL indicator
            char nullBit = 0;
            memcpy(dataItr, &nullBit, sizeof(char));
            dataItr += sizeof(char);
            if (recordDescriptor[requiredIndex].type == TypeInt) {
//                    std::cout<<"292"<<std::endl;
                memcpy(dataItr, valuePtr, sizeof(int));
                dataItr += sizeof(int);
            } else if (recordDescriptor[requiredIndex].type == TypeVarChar) {
//                    std::cout<<"302";
                memcpy(dataItr, &requiredDataLength, sizeof(int));
                dataItr += sizeof(int);
                memcpy(dataItr, valuePtr, requiredDataLength);
                dataItr += requiredDataLength;
            } else if (recordDescriptor[requiredIndex].type == TypeReal) {
//                    std::cout<<"297";
                memcpy(dataItr, valuePtr, sizeof(float));
                dataItr += sizeof(float);
            }
        } else {
            //its a NULL
            char reBytePtr = 0;
            (reBytePtr) |= (1 << 7);
            memcpy(dataItr, &reBytePtr, sizeof(char));
        }
        return 0;
    }

    RC RBFM_ScanIterator::prepareSelectedAttributesRecord(FileHandle &fileHandle,
                                                          const std::vector<Attribute> &recordDescriptor,
                                                          const RID &rid, void *data,
                                                          std::vector<int> filteredNeededAttributeList) {

        char *dataItr = (char *) data;
        char *pageBufferPtr = (char *) malloc(sizeof(char) * PAGE_SIZE);
        // check if page exists
        if (fileHandle.readPage(rid.pageNum, pageBufferPtr) == -1) {
            free(pageBufferPtr);
            pageBufferPtr = NULL;
            return FAILED;
        }

        // Calculating index of the bytes location
        char *recordMetaStart =
                pageBufferPtr + PAGE_SIZE - 2 * sizeof(int) - rid.slotNum * (2 * sizeof(int) + sizeof(char));

        char *tombStartLocation = recordMetaStart;
        char *recordPtrPtr = tombStartLocation + sizeof(char);
        int pointerLocation = *((int *) (recordPtrPtr));
        char *recordStartPointer = pageBufferPtr + pointerLocation;

        int numFieldsInRecord = *((int *) recordStartPointer);
        int nullIndicatorSize = ceil((float) numFieldsInRecord / 8);
        char *nullIndicatorPtr = recordStartPointer + sizeof(int);
        char *pointerArrayStart = nullIndicatorPtr + nullIndicatorSize;


        //set bit map
        int counter = 0;

        int outputNullIndicatorSize = ceil((float) filteredNeededAttributeList.size() / 8);
        memset(dataItr,0,outputNullIndicatorSize);

        for (auto index: filteredNeededAttributeList) {
            int reqByteNum = counter / 8;
            int bitToSetInByte = 7 - (counter % 8);
            char *reBytePtr = dataItr + reqByteNum * (sizeof(char));
            if (rbfManagerPtr->checkBitSet(index, nullIndicatorPtr)) {
                (*reBytePtr) |= (1 << bitToSetInByte);
            } else {
                (*reBytePtr) &= ~(1 << bitToSetInByte);
            }
            counter++;
        }

        dataItr += outputNullIndicatorSize;

        //copying data
        for (auto index: filteredNeededAttributeList) {
            if (!rbfManagerPtr->checkBitSet(index, nullIndicatorPtr)) {

                // shift pointer array start to required pointer location
                char *currentIndexPtr = pointerArrayStart + index * sizeof(int);
                char *previousIndexPtr = pointerArrayStart + (index - 1) * sizeof(int);
                int previousIndexOffset;
                if (index == 0) {
                    previousIndexOffset = sizeof(int) + nullIndicatorSize + sizeof(int) * numFieldsInRecord - 1;
                } else {
                    previousIndexOffset = (*((int *) previousIndexPtr));
                }
                char *valuePtr = recordStartPointer + previousIndexOffset + 1;
                int length = *((int *) currentIndexPtr) - previousIndexOffset;
                if (recordDescriptor[index].type == TypeInt) {
//                    std::cout<<"292"<<std::endl;
                    memcpy(dataItr, valuePtr, sizeof(int));
                    dataItr += sizeof(int);
                } else if (recordDescriptor[index].type == TypeVarChar) {
//                    std::cout<<"302";
                    memcpy(dataItr, &length, sizeof(int));
                    dataItr += sizeof(int);
                    memcpy(dataItr, valuePtr, length);
                    dataItr += length;
                } else if (recordDescriptor[index].type == TypeReal) {
//                    std::cout<<"297";
                    memcpy(dataItr, valuePtr, sizeof(float));
                    dataItr += sizeof(float);
                }
            }
        }
        free(pageBufferPtr);
        pageBufferPtr = NULL;
        return PASS;

    }

    RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                             const RID &rid, const std::string &attributeName, void *data) {
        //get page
        char *pageReadBuffer = (char *) malloc(PAGE_SIZE * sizeof(char));
        int RC = fileHandle.readPage(rid.pageNum, pageReadBuffer);
        RID finalRID;

        if (RC == -1) {
            free(pageReadBuffer);
            pageReadBuffer = NULL;
            return FAILED;
        }
        //get final RID to read
        getFinalRID(fileHandle, pageReadBuffer, recordDescriptor, rid, finalRID);

        //read attribute now
        readFinalAttribute(pageReadBuffer, recordDescriptor, finalRID, attributeName, data);
        free(pageReadBuffer);
        pageReadBuffer = NULL;
        return 0;
    }


    void RBFM_ScanIterator::populateIterator(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                             const std::string &conditionAttribute, const CompOp compOp,
                                             const void *value, const std::vector<std::string> &attributeNames) {
        this->fileHandle = fileHandle;
        this->recordDescriptor = recordDescriptor;
        this->conditionAttribute = conditionAttribute;
        this->compOp = compOp;
        this->expectedValueRecord = value;
        this->requestedAttributeNames = attributeNames;
        this->pageNum = 0;
        //TODO::changed slotnum here
        this->slotNum = 1;
        //Not yet initialised
        this->valueInIterator = NULL;

        this->filteredNeededAttributeList.clear();
        this->presentPage = NULL;
        this->totalNumPageInFile = this->fileHandle.getNumberOfPages();


        //read page
        this->presentPage = (char *) malloc(PAGE_SIZE);
        this->fileHandle.readPage(this->pageNum, this->presentPage);
        char *numSlotsPtr = this->presentPage + PAGE_SIZE - 2 * (sizeof(int));
        this->totalSlotsInCurrentPage = *((int *) numSlotsPtr);
        this->valueInIteratorType = TypeVarChar;
    }

    RBFM_ScanIterator::RBFM_ScanIterator() {
        this->recordDescriptor.clear();
        this->conditionAttribute.clear();
        this->compOp = EQ_OP;
        this->expectedValueRecord = 0;

        this->expectedValueRecord = NULL;
        this->valueInIterator = NULL;
        this->valueInIteratorType = TypeVarChar;
        this->filteredNeededAttributeList.clear();
        this->rbfManagerPtr = &RecordBasedFileManager::instance();
        this->totalSlotsInCurrentPage = 0;
    }

    RBFM_ScanIterator::~RBFM_ScanIterator() {
        void *valueInIterator = NULL;
        AttrType valueInIteratorType = TypeVarChar;
    }

    bool RBFM_ScanIterator::IsnullBitMapSet(char *nullBitMap) {
        return *nullBitMap & 1;
    }

    bool RBFM_ScanIterator::handleEmptyExpectedRecordComparision(void *Value) {
        //No value in expected record case
        if (this->compOp == NO_OP) {
            return true;
        }
        std::set<int> eqSet = {EQ_OP, LE_OP, GE_OP, NE_OP};
        bool IsPresentInSet = (eqSet.find(this->compOp) != eqSet.end());
        if (IsPresentInSet)
            return Value == NULL;
        return false;

    }


    bool RBFM_ScanIterator::handleNullMembers(char *expectedRecordDataPtr) {
        if (expectedRecordDataPtr == NULL) {
            return handleEmptyExpectedRecordComparision(this->valueInIterator);
        } else if (this->valueInIterator == NULL) {
            return handleEmptyExpectedRecordComparision(expectedRecordDataPtr);
        }
        return false;
    }

    RC RBFM_ScanIterator::floatCompare(float actual, float expected) {
        if (fabs(actual - expected) < DELTA) {
            return 0;
        } else if (actual < expected) {
            return -1;
        } else {
            return 1;
        }
    }

    bool RBFM_ScanIterator::recordCompare(const void *expectedRecord) {
        if (this->compOp == NO_OP){
            return true;
        }
        char *expectedRecordDataPtr = (char *) expectedRecord;

        //For every record
        char *currentRecordNullBitMapPtr = (char *) this->valueInIterator;
        char *currentRecordDataPtr = currentRecordNullBitMapPtr + sizeof(char);
        //memcpy(currentRecordNullBitMap, expectedRecord, sizeof(char));

        this->valueInIterator = (char *) this->valueInIterator + sizeof(char);
        if (*currentRecordNullBitMapPtr) {
            this->valueInIterator = NULL;
        }
        //Handle empty cases
        if (expectedRecord == NULL || this->valueInIterator == NULL) {
            return handleNullMembers(expectedRecordDataPtr);
        }

        int actualCompValue = 0;
        if (this->valueInIteratorType == TypeInt) {
            actualCompValue = *((int *) this->valueInIterator) - *((int *) expectedRecordDataPtr);

        } else if (this->valueInIteratorType == TypeReal) {
            actualCompValue = floatCompare(*((float *) this->valueInIterator), *((float *) expectedRecordDataPtr));
        } else if (this->valueInIteratorType == TypeVarChar) {
            //original string (expects null terminated string)
            int lengthOriginalVarchar = *((int *) this->valueInIterator);
            char *originalVarcharPtr = (char *) this->valueInIterator + sizeof(int);
            char originalString[lengthOriginalVarchar + 1];
            originalString[lengthOriginalVarchar] = '\0';
            memcpy(originalString, originalVarcharPtr, lengthOriginalVarchar);


            //Fetching expected string
            int lengthExpectedVarchar = *((int *) expectedRecordDataPtr);
            char *expectedVarcharPtr = expectedRecordDataPtr + sizeof(int);
            char expectedString[lengthExpectedVarchar + 1];
            expectedString[lengthExpectedVarchar] = '\0';
            memcpy(expectedString, expectedVarcharPtr, lengthExpectedVarchar);
            actualCompValue = strcmp(originalString, expectedString);
        }
        //free(nullBitMap);
        switch (this->compOp) {
            case EQ_OP:
                return actualCompValue == 0;
            case LT_OP:
                return actualCompValue < 0;
            case LE_OP:
                return actualCompValue <= 0;
            case GT_OP:
                return actualCompValue > 0;
            case GE_OP:
                return actualCompValue >= 0;
            case NE_OP:
                return actualCompValue != 0;
            case NO_OP:
                return true;
            default:
                return false;
        }
        return false;
    }

    RC RecordBasedFileManager::populateFilteredList(RBFM_ScanIterator &rbfm_ScanIterator,
                                                    const std::vector<Attribute> &recordDescriptor,
                                                    const std::vector<std::string> &attributeNames) {
        //Take intersection of needed and present records to get common record attributes
        for (auto requestedAttribute: attributeNames) {
            int index = 0;
            for (auto presentAttribute: recordDescriptor) {
                if (presentAttribute.name.compare(requestedAttribute) == 0) {
                    rbfm_ScanIterator.filteredNeededAttributeList.push_back(index);
                    break;
                }
                index++;
            }
        }
        return PASS;
    }

    void RecordBasedFileManager::setFilterAttrIndex(int *index, const std::vector<Attribute> &recordDescriptor,
                                                    const std::string &conditionAttribute) {
        int counter = 0;
        for (auto record = recordDescriptor.begin(); record != recordDescriptor.end(); ++record) {
            if (record->name.compare(conditionAttribute) == 0) {
                *index = counter;
                return;
            }
            counter++;
        }
        *index = -1;
    }

    RC RecordBasedFileManager::scan(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                    const std::string &conditionAttribute, const CompOp compOp, const void *value,
                                    const std::vector<std::string> &attributeNames,
                                    RBFM_ScanIterator &rbfm_ScanIterator) {
        rbfm_ScanIterator.populateIterator(fileHandle, recordDescriptor, conditionAttribute, compOp, value,
                                           attributeNames);
        populateFilteredList(rbfm_ScanIterator, recordDescriptor, attributeNames);
        setFilterAttrIndex(&rbfm_ScanIterator.filterAttrIndex, recordDescriptor, conditionAttribute);
        if (rbfm_ScanIterator.filterAttrIndex > -1) {
            rbfm_ScanIterator.valueInIteratorType = recordDescriptor[rbfm_ScanIterator.filterAttrIndex].type;
            int *length = &rbfm_ScanIterator.valueLength;
            if (rbfm_ScanIterator.valueInIteratorType == TypeInt) {
                *length = sizeof(int);
            } else if (rbfm_ScanIterator.valueInIteratorType == TypeReal) {
                *length = sizeof(float);
            } else {
                char *varChar = (char *) value;
                *length = *((int *) varChar);
            }
        }
        return PASS;
    }


    RC RBFM_ScanIterator::updateIterator() {
        //load new  page
        if(this->presentPage) {
            free(this->presentPage);
            this->presentPage = NULL;
        }
        this->pageNum++;
        this->slotNum = 1;
        if (this->pageNum > this->totalNumPageInFile) {
            return RBFM_EOF;
        }
        this->presentPage = (char *) malloc(PAGE_SIZE);
        this->fileHandle.readPage(this->pageNum, this->presentPage);
        char *numSlotsPtr = presentPage + PAGE_SIZE - 2 * (sizeof(int));
        this->totalSlotsInCurrentPage = *((int *) numSlotsPtr);
        return PASS;
    }

    RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data) {

        //for a given page get metadata of the slot
        //get tomb stone data for this record
        char recordTombStoneByte;
        rbfManagerPtr->getTombStoneData(presentPage, &recordTombStoneByte, this->slotNum);

        // if record is already deleted , return error
        bool isNodeDeleted = true;
        LinkedListPosition nodeType = LinkedListStart;
        bool filterConditionMet = false;
        while (isNodeDeleted || nodeType == LinkedListStart || nodeType == LinkedListMiddle || !filterConditionMet) {
            //Go to next page 1st slot if slot num exceeded
            if (this->slotNum > this->totalSlotsInCurrentPage) {
                int RC = updateIterator();
                if (RC == RBFM_EOF) {
                    return RBFM_EOF;
                }
            }

            char recordTombStoneByte;
            rbfManagerPtr->getTombStoneData(this->presentPage, &recordTombStoneByte, this->slotNum);
            isNodeDeleted = rbfManagerPtr->getBitValueInTombStoneByte(recordTombStoneByte, NodeDeletionIndicator);
            if (isNodeDeleted) {
                this->slotNum++;
                continue;
            }
            bool isRecordTombStone = rbfManagerPtr->getBitValueInTombStoneByte(recordTombStoneByte, TombStone);
            if (isRecordTombStone) {
                nodeType = rbfManagerPtr->getLinkedListNodeType(recordTombStoneByte);
                if (nodeType != LinkedListEnd) {
                    this->slotNum++;
                    continue;
                }
            } else {
                nodeType = NotLinkedListNode;
            }

            //checking filter condition
            //read record and send only filtered attributes
            RID tmpRID;
            tmpRID.pageNum = this->pageNum;
            tmpRID.slotNum = this->slotNum;
            PageMetaData pageMeta;
            rbfManagerPtr->populatePageMetaData(this->presentPage, tmpRID, pageMeta);

            //read required attribute

            this->valueLength = rbfManagerPtr->getRecordSizeFromSlot(this->slotNum, this->presentPage);
            void* buffPtr = malloc(this->valueLength);
            this->valueInIterator = buffPtr;
            int RC = rbfManagerPtr->readAttribute(this->fileHandle, this->recordDescriptor, tmpRID,
                                                  this->conditionAttribute, this->valueInIterator);
            // TODO :: assuming that conditional attribute should always be present
            if (RC == FAILED) {
                return FAILED;
            }
            filterConditionMet = recordCompare(this->expectedValueRecord);
            if (buffPtr) {
                free(buffPtr);
                buffPtr = NULL;
            }
            if (!filterConditionMet) {
                this->slotNum++;
                continue;
            }
        }

        //read record and send only filtered attributes
        rid.pageNum = this->pageNum;
        rid.slotNum = this->slotNum;

        //Populate output data
        prepareSelectedAttributesRecord(this->fileHandle, this->recordDescriptor, rid, data,
                                        this->filteredNeededAttributeList);
        //load new state to iterator
        this->slotNum++;
        if (this->slotNum > this->totalSlotsInCurrentPage) {
            int RC = updateIterator();
            if (RC == RBFM_EOF) {
                return RBFM_EOF;
            }
        }
        return PASS;
    }

    RC RBFM_ScanIterator::close() {
        this->pageNum = 0;
        this->slotNum = 0;
        this->valueInIterator = NULL;
        this->filteredNeededAttributeList.clear();
        this->presentPage = NULL;
        return rbfManagerPtr->closeFile(this->fileHandle);
        return PASS;
    }
} // namespace PeterDB
