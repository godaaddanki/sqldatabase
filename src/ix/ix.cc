#include "src/include/ix.h"

namespace PeterDB {
    IndexManager &IndexManager::instance() {
        static IndexManager _index_manager = IndexManager();
        return _index_manager;
    }

    static PagedFileManager *pfmManagerPtr = &PagedFileManager::instance();

    RC IndexManager::getPageMetadata(const void *pagePtr, IXPageMeta &pageMeta) {
        if (!pagePtr) {
            return FAILED;
        }
        char *pageStart = (char *) pagePtr;
        char *freeSpacePtr = pageStart + PAGE_SIZE - sizeof(int);
        pageMeta.freeSpace = *((int *) freeSpacePtr);
        char *numSlotsPtr = freeSpacePtr - sizeof(int);
        pageMeta.numSlots = *((int *) numSlotsPtr);
        char *typePtr = numSlotsPtr - sizeof(char);
        pageMeta.nodeType = (IXNodeType) (*typePtr);
        // printf("%d node type", pageMeta.nodeType);
        char *prevPtr = typePtr - sizeof(int);
        pageMeta.prevPagePtr = *((int *) prevPtr);
        char *nextPtr = prevPtr - sizeof(int);
        pageMeta.nextPagePtr = *((int *) nextPtr);
        return PASS;
    }

    RC IndexManager::setPageMetadata(void *pagePtr, IXPageMeta &pageMeta) {
        char *pageStart = (char *) pagePtr;
        char *freeSpacePtr = pageStart + PAGE_SIZE - sizeof(int);
        *((int *) freeSpacePtr) = pageMeta.freeSpace;
        char *numSlotsPtr = freeSpacePtr - sizeof(int);
        *((int *) numSlotsPtr) = pageMeta.numSlots;
        char *typePtr = numSlotsPtr - sizeof(char);
        *typePtr = pageMeta.nodeType;
        char *prevPtr = typePtr - sizeof(int);
        *((int *) prevPtr) = pageMeta.prevPagePtr;
        char *nextPtr = prevPtr - sizeof(int);
        *((int *) nextPtr) = pageMeta.nextPagePtr;
        return PASS;
    }

    RC IndexManager::initPageMetadata(void *pagePtr, IXNodeType nodeType) {
        char *pageStart = (char *) pagePtr;
        char *freeSpacePtr = pageStart + PAGE_SIZE - sizeof(int);
        int pageMetaSize = (nodeType == LeafNode) ? PAGE_META_SIZE : NON_LEAF_META_SIZE;
        *((int *) freeSpacePtr) = PAGE_SIZE - pageMetaSize;
        char *numSlotsPtr = freeSpacePtr - sizeof(int);
        *((int *) numSlotsPtr) = 0;
        char *typePtr = numSlotsPtr - sizeof(char);
        *typePtr = nodeType;
        char *prevPtr = typePtr - sizeof(int);
        *((int *) prevPtr) = -5;
        char *nextPtr = prevPtr - sizeof(int);
        *((int *) nextPtr) = -5;
        return PASS;
    }

    RC IndexManager::createFile(const std::string &fileName) {
        if (pfmManagerPtr->createFile(fileName) == FAILED) {
            return FAILED;
        }
        return PASS;
    }

    RC IndexManager::destroyFile(const std::string &fileName) {
        return pfmManagerPtr->destroyFile(fileName);
    }

    RC IndexManager::openFile(const std::string &fileName, IXFileHandle &ixFileHandle) {
        if (ixFileHandle.fileHandle.activeFile != NULL) {
            return -1;
        }
        return pfmManagerPtr->openFile(fileName, ixFileHandle.fileHandle);
    }

    RC IndexManager::closeFile(IXFileHandle &ixFileHandle) {
        return pfmManagerPtr->closeFile(ixFileHandle.fileHandle);
    }

    int IndexManager::getDataEntrySize(const Attribute &attribute, const void *key) const {
        if (attribute.type == TypeInt) {
            return sizeof(int) + sizeof(unsigned) + sizeof(unsigned short);
        } else if (attribute.type == TypeReal) {
            return sizeof(float) + sizeof(unsigned) + sizeof(unsigned short);
        } else {
            return *((int *) key) + sizeof(int) + sizeof(unsigned) + sizeof(unsigned short);
        }
    }

    int IndexManager::getIndexEntrySize(const Attribute &attribute, const void *key) const {
        if (attribute.type == TypeInt) {
            return sizeof(int) + sizeof(int);
        } else if (attribute.type == TypeReal) {
            return sizeof(float) + sizeof(int);
        } else {
            return *((int *) key) + sizeof(int) + sizeof(int);
        }
    }

    int IndexManager::floatCompare(float inputKey, float keyInNode) const {
        if (fabs(inputKey - keyInNode) < DELTA) {
            return 0;
        } else if (inputKey < keyInNode) {
            return -1;
        } else {
            return 1;
        }
    }

    RC IndexManager::getRootPageNum(IXFileHandle &ixFileHandle, int &rootPageNum) {
        void *dummyPage = malloc(PAGE_SIZE);
        if (ixFileHandle.fileHandle.readPage(DUMMY_PAGE_FOR_ROOT, dummyPage) == FAILED) {
            loadedFree(&dummyPage);
            return FAILED;
        }
        rootPageNum = *((int *) dummyPage);
        loadedFree(&dummyPage);
        return PASS;
    }


    RC IndexManager::setRootPageNum(IXFileHandle &ixFileHandle, int rootPageNum) {
        void *dummyPage = malloc(PAGE_SIZE);
        if (ixFileHandle.fileHandle.readPage(DUMMY_PAGE_FOR_ROOT, dummyPage) == FAILED) {
            loadedFree(&dummyPage);
            return FAILED;
        }
        *((int *) dummyPage) = rootPageNum;
        if (ixFileHandle.fileHandle.writePage(DUMMY_PAGE_FOR_ROOT, dummyPage) == FAILED) {
            loadedFree(&dummyPage);
            return FAILED;
        }
        loadedFree(&dummyPage);
        return PASS;
    }

    RC IndexManager::getLeafNodeForGivenKey(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key,
                                            int rootPageNum, int &leafPageNum, char *pagePtr) {

        IXPageMeta pageMeta;
        int subTreePageNum = 0;
        getPageMetadata(pagePtr, pageMeta);
        if (pageMeta.nodeType == LeafNode) {
            leafPageNum = rootPageNum;
            return PASS;
        }
        if (pageMeta.nodeType != LeafNode) {
            // get subtree for the key recursively till we get leafnode
            subTreePageNum = getNextSubTreeNode(ixFileHandle, attribute, key, pagePtr);
            if (subTreePageNum == FAILED) {
                return FAILED;
            }
            if (ixFileHandle.fileHandle.readPage(subTreePageNum, pagePtr) == FAILED) {
                return FAILED;
            }
            getLeafNodeForGivenKey(ixFileHandle, attribute, key, subTreePageNum, leafPageNum, (char *) pagePtr);
        }
        return PASS;
    }


    int IndexManager::compareKey(const Attribute &attribute, const void *inputKey, const void *keyInNode) const {
        if (attribute.type == TypeInt) {
            return (*(int *) inputKey - *(int *) keyInNode);
        } else if (attribute.type == TypeReal) {
            return floatCompare(*(float *) inputKey, *(float *) keyInNode);
        } else {
            //inputKey
            int lengthInputKeyVarchar = *((int *) inputKey);
            char *inputKeyVarcharPtr = (char *) inputKey + sizeof(int);
            char inputKeyString[lengthInputKeyVarchar + 1];
            inputKeyString[lengthInputKeyVarchar] = '\0';
            memcpy(inputKeyString, inputKeyVarcharPtr, lengthInputKeyVarchar);

            //keyInNode
            int lengthKeyInNodeVarchar = *((int *) keyInNode);
            char *keyInNodeVarcharPtr = (char *) keyInNode + sizeof(int);
            char keyInNodeString[lengthKeyInNodeVarchar + 1];
            keyInNodeString[lengthKeyInNodeVarchar] = '\0';
            memcpy(keyInNodeString, keyInNodeVarcharPtr, lengthKeyInNodeVarchar);
            return strcmp(inputKeyString, keyInNodeString);
        }
    }

    RC IndexManager::insertDataEntryInLeafNode(char *pagePtr, const Attribute &attribute, const void *key,
                                               const RID &rid) {
        //        If free space is sufficeint -
        //                         Find right slot to insert new key RID, as leaf is sorted. Shift the other slots --
        //        Update page meta info
        //
        IXPageMeta pageMeta;
        getPageMetadata(pagePtr, pageMeta);
        int inputDataEntrySize = getDataEntrySize(attribute, key);
        if (pageMeta.freeSpace < inputDataEntrySize) {
            return FAILED;
        }
        int insertSlotPtr = 0;
        char *ridPageNumPtr;

        //Get index to insert the record
        if (pageMeta.numSlots != 0) {
            char *dataEntryItr = pagePtr;
            int sizeSoFar = 0;
            int i;
            for (i = 0; i < pageMeta.numSlots; i++) {
                if (compareKey(attribute, key, dataEntryItr) < 0) {
                    break;
                }
                int dataEntrySize = getDataEntrySize(attribute, dataEntryItr);
                dataEntryItr += dataEntrySize;
                sizeSoFar += dataEntrySize;
            }
            insertSlotPtr = sizeSoFar;

            if (i < pageMeta.numSlots) {
                //Make room for the new record
                int start = sizeSoFar;
                int end = PAGE_SIZE - PAGE_META_SIZE - pageMeta.freeSpace;
                int sizeOfChunkToMove = end - start;
                memmove(pagePtr + insertSlotPtr + inputDataEntrySize, pagePtr + insertSlotPtr, sizeOfChunkToMove);
            }
        }

        // insert new data entry
        char *recordStartPointer = pagePtr + insertSlotPtr;
        if (attribute.type == TypeInt) {
            *((int *) recordStartPointer) = *((int *) key);
            ridPageNumPtr = recordStartPointer + sizeof(int);
        } else if (attribute.type == TypeReal) {
            *((float *) recordStartPointer) = *((float *) key);
            ridPageNumPtr = recordStartPointer + sizeof(float);
        } else {
            int varCharLength = *((int *) key);
            *((int *) recordStartPointer) = varCharLength;
            char *varCharStartPtr = recordStartPointer + sizeof(int);
            memcpy(varCharStartPtr, (char *) key + sizeof(int), varCharLength);
            ridPageNumPtr = varCharStartPtr + varCharLength;
        }
        *((unsigned *) ridPageNumPtr) = rid.pageNum;
        char *ridSlotPtr = ridPageNumPtr + sizeof(unsigned);
        *((unsigned short *) ridSlotPtr) = rid.slotNum;
        //update page meta after insert data entry
        pageMeta.numSlots++;
        pageMeta.freeSpace -= inputDataEntrySize;
        setPageMetadata(pagePtr, pageMeta);
        // printf("in insert leaf entry end");
        return PASS;
    }

    RC IndexManager::deleteDataEntryInLeafNode(char *pagePtr, const Attribute &attribute, const void *key,
                                               const RID &rid) {
        IXPageMeta pageMeta;
        getPageMetadata(pagePtr, pageMeta);
        int inputDataEntrySize = getDataEntrySize(attribute, key);
        int deleteSlotPtr = 0;
        int numSlotsCounter = 0;
        int i = 0;
        // printf("num slots %d", pageMeta.numSlots);
        //Get index to insert the record
        if (pageMeta.numSlots != 0) {
            char *dataEntryItr = pagePtr;
            int sizeSoFar = 0;
            for (i = 0; i < pageMeta.numSlots; i++) {
                numSlotsCounter += 1;
                if (compareKey(attribute, key, dataEntryItr) == 0) {
                    // check if RID also matches, because same key can have multiple RIDs
                    int dataEntrySize = getDataEntrySize(attribute, dataEntryItr);
                    int sizeOfKey = dataEntrySize - sizeof(unsigned) - sizeof(unsigned short);

                    RID leafNodeRid;
                    leafNodeRid.pageNum = *((unsigned *) (dataEntryItr + sizeOfKey));
                    leafNodeRid.slotNum = *((unsigned short *) (dataEntryItr + sizeOfKey + sizeof(unsigned)));
                    // printf("%d rid,%d slotnum \n", leafNodeRid.slotNum, leafNodeRid.pageNum);
                    //  printf("%d rid,%d slotnum \n", rid.slotNum, rid.pageNum);
                    if (leafNodeRid.slotNum == rid.slotNum && leafNodeRid.pageNum == rid.pageNum) {
                        //    printf("rid,slotnum same");
                        break;
                    }
                }
                int dataEntrySize = getDataEntrySize(attribute, dataEntryItr);
                dataEntryItr += dataEntrySize;
                sizeSoFar += dataEntrySize;
            }

            // check if its valid key to delete
            if (i == pageMeta.numSlots) {
                // no of slots  0 i.e its an empty page or key doesnt match with any data entries to delete
//                printf("shouldnt come here");
                return FAILED;
            }

            deleteSlotPtr = sizeSoFar;
            //shrink after deleting record
            int start = sizeSoFar;
            int end = PAGE_SIZE - PAGE_META_SIZE - pageMeta.freeSpace;
            int sizeOfChunkToMove = end - start;
            memmove(pagePtr + deleteSlotPtr, pagePtr + deleteSlotPtr + inputDataEntrySize, sizeOfChunkToMove);
        } else {
            return FAILED;
        }
        //update page meta after insert data entry
        pageMeta.numSlots--;
        pageMeta.freeSpace += inputDataEntrySize;
        setPageMetadata(pagePtr, pageMeta);
        return PASS;
    }

    RC IndexManager::insertIndexEntryInIndexNode(char *pagePtr, const Attribute &attribute, const void *key,
                                                 int childPageNum) {
        IXPageMeta pageMeta;
        getPageMetadata(pagePtr, pageMeta);
        int inputIndexEntrySize = getIndexEntrySize(attribute, key);
        if (pageMeta.freeSpace < inputIndexEntrySize) {
            return FAILED;
        }

        //first 4 bytes for left pointer
        int insertSlotPtr = sizeof(int);
        char *childPageNumPtr;

        //Get index to insert the record
        if (pageMeta.numSlots != 0) {
            char *indexEntryItr = pagePtr + sizeof(int);
            int sizeSoFar = sizeof(int);
            for (int i = 0; i < pageMeta.numSlots; i++) {
                if (compareKey(attribute, key, indexEntryItr) < 0) {
                    break;
                }
                int indexEntrySize = getIndexEntrySize(attribute, indexEntryItr);
                indexEntryItr += indexEntrySize;
                sizeSoFar += indexEntrySize;
            }
            insertSlotPtr = sizeSoFar;

            //Make room for the new record
            int start = sizeSoFar;
            int end = PAGE_SIZE - PAGE_META_SIZE - pageMeta.freeSpace;
            int sizeOfChunkToMove = end - start;
            memmove(pagePtr + insertSlotPtr + inputIndexEntrySize, pagePtr + insertSlotPtr, sizeOfChunkToMove);
        }

        // insert new data entry
        char *recordStartPointer = pagePtr + insertSlotPtr;
        if (attribute.type == TypeInt) {
            *((int *) recordStartPointer) = *((int *) key);
            childPageNumPtr = recordStartPointer + sizeof(int);
        } else if (attribute.type == TypeReal) {
            *((float *) recordStartPointer) = *((float *) key);
            childPageNumPtr = recordStartPointer + sizeof(float);
        } else {
            int varCharLength = *((int *) key);
            *((int *) recordStartPointer) = varCharLength;
            char *varCharStartPtr = recordStartPointer + sizeof(int);
            memcpy(varCharStartPtr, (char *) key + sizeof(int), varCharLength);
            childPageNumPtr = varCharStartPtr + varCharLength;
        }
        *((int *) childPageNumPtr) = childPageNum;

        //update page meta after insert data entry
        pageMeta.numSlots++;
        pageMeta.freeSpace -= inputIndexEntrySize;
        setPageMetadata(pagePtr, pageMeta);
        return PASS;
    }

    RC IndexManager::deleteIndexEntryInIndexNode(char *pagePtr, const Attribute &attribute, const void *key,
                                                 int childPageNum) {
        IXPageMeta pageMeta;
        getPageMetadata(pagePtr, pageMeta);
        int inputIndexEntrySize = getIndexEntrySize(attribute, key);
        int deleteSlotPtr = 0;
        int numSlotsCounter = 0;

        //Get index to insert the record
        if (pageMeta.numSlots != 0) {
            char *indexEntryItr = pagePtr;
            int sizeSoFar = 0;
            for (int i = 0; i < pageMeta.numSlots; i++) {
                if (compareKey(attribute, key, indexEntryItr) == 0) {
                    break;
                }
                int indexEntrySize = getIndexEntrySize(attribute, indexEntryItr);
                indexEntryItr += indexEntrySize;
                sizeSoFar += indexEntrySize;
                numSlotsCounter += 1;
            }

            // check if its valid key to delete
            if (numSlotsCounter == 0 || numSlotsCounter == pageMeta.numSlots) {
                // no of slots  0 i.e its an empty page or key doesnt match with any data entries to delete
                return FAILED;
            }

            deleteSlotPtr = sizeSoFar;
            //Make room for the new record
            int start = sizeSoFar;
            int end = PAGE_SIZE - PAGE_META_SIZE - pageMeta.freeSpace;
            int sizeOfChunkToMove = end - start;
            memmove(pagePtr + deleteSlotPtr, pagePtr + deleteSlotPtr + inputIndexEntrySize, sizeOfChunkToMove);
        }

        //update page meta after insert data entry
        pageMeta.numSlots--;
        pageMeta.freeSpace += inputIndexEntrySize;
        setPageMetadata(pagePtr, pageMeta);
        return PASS;
    }

    RC
    IndexManager::setupBTree(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid) {
        //        New page - actual root
        //        Fill in meta data for the root node
        //
        //        New page - leaf node
        //        Fill in. meta data
        //        Fill in actual key pair
        //Create dummy node for root
        void *dummyPage = malloc(PAGE_SIZE);
        initPageMetadata(dummyPage, DummyNode);
        *((int *) dummyPage) = 1;
        if (ixFileHandle.fileHandle.appendPage(dummyPage) == FAILED) {
            loadedFree(&dummyPage);
            return FAILED;
        }
        loadedFree(&dummyPage);

        //Create root page which is just a leaf for the 1st entry.
        void *rootPage = malloc(PAGE_SIZE);
        initPageMetadata(rootPage, LeafNode);
        insertDataEntryInLeafNode((char *) rootPage, attribute, key, rid);
        if (ixFileHandle.fileHandle.appendPage(rootPage) == FAILED) {
            loadedFree(&rootPage);
            return FAILED;
        }
        std::stringstream out;
        //printBTree(ixFileHandle, attribute,out);
        // printf("in setup B tree");

        loadedFree(&rootPage);
        return PASS;
    }

    SubTreePageNum
    IndexManager::getNextSubTreeNode(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key,
                                     const void *presentPage) {
        //        Frm the current page getChildPage -
        //                             In the current page info, find num slots - each slot points to child pointer and key ,but we have one extra pointer . So beore left most or right most pointer seperately
        //
        //        Based on required key and index key comparision, find the required slotnumber
        //        Get the dhild page based on it
        IXPageMeta pageMeta;
        getPageMetadata(presentPage, pageMeta);
        if (pageMeta.nodeType == LeafNode) {
            return FAILED;
        }
        if (pageMeta.numSlots == 0) {
//            // std::cout << "No entries in the index node cannot insert a node" << std::endl;
            return FAILED;
        }

        // if key is NULL, we need leftmost leafpage,so return leftmost pointer of the page
        if (key == NULL) {
            int pageNum = *((int *) presentPage);
            return pageNum;
        }

        char *indexEntryItr = (char *) presentPage + sizeof(int);
        SubTreePageNum childPage = -1;
        for (int i = 0; i < pageMeta.numSlots; i++) {
            if (compareKey(attribute, key, indexEntryItr) < 0) {
                char *leftChildPtr = indexEntryItr - sizeof(int);
                childPage = *((int *) leftChildPtr);
                return childPage;
            }
            int indexEntrySize = getIndexEntrySize(attribute, indexEntryItr);
            indexEntryItr += indexEntrySize;
        }
        char *rightChildPtr = indexEntryItr - sizeof(int);
        childPage = *((int *) rightChildPtr);
        return childPage;
    }

    int
    IndexManager::adjustSizeBasedOnSameKey(const void *keyToBeInserted, const void *midKey, const Attribute &attribute,
                                           char *dataEntryItr, IXPageMeta &pageMeta, int &index) {
        int sizeSoFar = 0;
        int start = 0;
        int end = 0;
        int startFound = false;
        bool isEndLast = false;
        int start_i = 0;
        int end_i = 0;
        for (int i = 0; i < pageMeta.numSlots; i++) {
            int dataEntrySize = getDataEntrySize(attribute, dataEntryItr);
            if (compareKey(attribute, midKey, dataEntryItr) == 0) {
                index = i;
                return sizeSoFar;
//                if (!startFound) {
//                    start = sizeSoFar;
//                    end = sizeSoFar + dataEntrySize;
//                    startFound = true;
//                    start_i = i;
//                    end_i = i + 1;
//                }
//                } else {
//                    end = sizeSoFar + dataEntrySize;
//                    end_i = i + 1;
//                    if (i == pageMeta.numSlots - 1) {
//                        isEndLast = true;
//                    }
//                }
            }
            dataEntryItr += dataEntrySize;
            sizeSoFar += dataEntrySize;
        }
//        int sizeKeyToBeInserted = getDataEntrySize(attribute, keyToBeInserted);
//        if (compareKey(attribute, keyToBeInserted, midKey) <= 0) {
//            int sizeTillEnd = PAGE_SIZE - PAGE_META_SIZE - pageMeta.freeSpace;
//            int freespaceKmIncluded = pageMeta.freeSpace + (sizeTillEnd - end);
//            if (freespaceKmIncluded > sizeKeyToBeInserted) {
//                index = end_i;
//                return end;
//            } else {
//                index = start_i;
//                return start;
//            }
//        } else {
//            int freespaceKmIncludedInRight = pageMeta.freeSpace + start;
//            if (freespaceKmIncludedInRight > sizeKeyToBeInserted) {
//                index = start_i;
//                return start;
//            } else {
//                index = end_i;
//                return end;
//            }
//        }
    }


    RC
    IndexManager::splitLeafNode(IXFileHandle &ixFileHandle, char *pagePtr, const Attribute &attribute, const void *key,
                                const RID &rid, childSplitInfo &newChildEntry, const int presentLeafPageNum) {
        IXPageMeta pageMeta;
        IXPageMeta splitPageMeta;
        getPageMetadata(pagePtr, pageMeta);
        int i = 0;

        if (pageMeta.numSlots == 0) {
//            // std::cout << "No entries in the leaf node cannot insert a node" << std::endl;
            return FAILED;
        }
        char *dataEntryItr = (char *) pagePtr;
        int hypoSize = 0;
        int sizeSoFar = 0;
        bool newKeyVisited = false;
        int keytoBeInsertedLength = getDataEntrySize(attribute, key);
        int hypotheticalMidLengthAfterInsertion =
                (PAGE_SIZE - PAGE_META_SIZE - pageMeta.freeSpace + keytoBeInsertedLength) / 2;
        //Get index where split should be done
        for (i = 0; i < pageMeta.numSlots; i++) {
            int dataEntrySize = getDataEntrySize(attribute, dataEntryItr);
            if ((compareKey(attribute, key, dataEntryItr) < 0) && !newKeyVisited) {
                hypoSize = sizeSoFar + keytoBeInsertedLength;
                if (hypoSize >= hypotheticalMidLengthAfterInsertion) {
                    break;
                }
                newKeyVisited = true;
            } else {
                hypoSize = hypoSize + dataEntrySize;
                if (hypoSize >= hypotheticalMidLengthAfterInsertion) {
                    break;
                }
            }
            dataEntryItr += dataEntrySize;
            sizeSoFar += dataEntrySize;
        }
        sizeSoFar = adjustSizeBasedOnSameKey(key, (char *) pagePtr + sizeSoFar, attribute, (char *) pagePtr, pageMeta,
                                             i);
        //       int sizeSoFar = sizeSoFarInitial;
        // create new page and Intialise meta
        void *splitNewPage = malloc(PAGE_SIZE);
        initPageMetadata(splitNewPage, LeafNode);
        getPageMetadata(splitNewPage, splitPageMeta);

        // Update meta and linkedlist nodes of new split page leaf and old page leaf
        // common data for both nodes
        int start = sizeSoFar;
        int end = PAGE_SIZE - PAGE_META_SIZE - pageMeta.freeSpace;
        int sizeOfChunkToMove = end - start;

        // Insert keys from old leaf to new leaf and set meta(new node)
        splitPageMeta.prevPagePtr = presentLeafPageNum;
        splitPageMeta.nextPagePtr = pageMeta.nextPagePtr;
        splitPageMeta.numSlots = pageMeta.numSlots - i;
        splitPageMeta.freeSpace = pageMeta.freeSpace + sizeSoFar;
        memmove(splitNewPage, pagePtr + sizeSoFar, sizeOfChunkToMove);
        setPageMetadata(splitNewPage, splitPageMeta);
        ixFileHandle.fileHandle.appendPage(splitNewPage);

        //old node
        memset(pagePtr + sizeSoFar, 0, sizeOfChunkToMove);
        pageMeta.nextPagePtr = ixFileHandle.fileHandle.getNumberOfPages() - 1;
        pageMeta.numSlots = i;
        pageMeta.freeSpace = PAGE_SIZE - PAGE_META_SIZE - sizeSoFar;
        setPageMetadata(pagePtr, pageMeta);
        ixFileHandle.fileHandle.writePage(presentLeafPageNum, pagePtr);

        //set newChildEntry
        int firstDataEntrySize = getDataEntrySize(attribute, splitNewPage);
        int firstKeyInSplitNodeSize = firstDataEntrySize - sizeof(unsigned) - sizeof(unsigned short);
        newChildEntry.key = malloc(firstKeyInSplitNodeSize);
        memcpy(newChildEntry.key, splitNewPage, firstKeyInSplitNodeSize);
        newChildEntry.pageNum = pageMeta.nextPagePtr;


        loadedFree(&splitNewPage);
        return PASS;
    }

    RC
    IndexManager::splitIndexNode(IXFileHandle &ixFileHandle, char *pagePtr, const Attribute &attribute, const RID &rid,
                                 childSplitInfo &newChildEntry, const int presentLeafPageNum) {
        IXPageMeta pageMeta;
        IXPageMeta splitPageMeta;
        getPageMetadata(pagePtr, pageMeta);
        int i = 0;

        if (pageMeta.numSlots == 0) {
//            // std::cout << "No entries in the index node cannot insert a node" << std::endl;
            return FAILED;
        }
        char *indexNodeItr = (char *) pagePtr + sizeof(int);
        int sizeSoFar = sizeof(int);
        int hypoSize = 0;
        int keytoBeInsertedLength = getIndexEntrySize(attribute, newChildEntry.key);
        bool isMidGoingUp = false;
        bool newKeyVisited = false;
        int hypotheticalMidLengthAfterInsertion =
                (PAGE_SIZE - PAGE_META_SIZE - pageMeta.freeSpace + keytoBeInsertedLength) / 2;
        //Get index where split should be done
        for (i = 0; i < pageMeta.numSlots; i++) {
            int indexEntrySize = getIndexEntrySize(attribute, indexNodeItr);
            //Considering new key in calculation
            if ((compareKey(attribute, newChildEntry.key, indexNodeItr) < 0) && !newKeyVisited) {
                hypoSize = sizeSoFar + keytoBeInsertedLength;
                if (hypoSize >= hypotheticalMidLengthAfterInsertion) {
                    isMidGoingUp = true;
                    break;
                }
                newKeyVisited = true;
            } else {
                hypoSize = hypoSize + indexEntrySize;
                if (hypoSize >= hypotheticalMidLengthAfterInsertion) {
                    break;
                }
            }
            indexNodeItr += indexEntrySize;
            sizeSoFar += indexEntrySize;
        }

//        sizeSoFar = adjustSizeInIndexNode(key, indexNodeItr, attribute, (char *) pagePtr + sizeof(int), pageMeta, i);

        // create new page and Intialise meta
        void *splitNewPage = malloc(PAGE_SIZE);
        initPageMetadata(splitNewPage, IndexNode);
        getPageMetadata(splitNewPage, splitPageMeta);

        //entry moving to top index
        int sizeRecordToBeMovingAbove = 0;
        if (isMidGoingUp) {
            sizeRecordToBeMovingAbove = keytoBeInsertedLength;
        } else {
            sizeRecordToBeMovingAbove = getIndexEntrySize(attribute, indexNodeItr);
        }


        //set newChildEntry
        int sizeKeyToBeMovingAbove = sizeRecordToBeMovingAbove - sizeof(int);
        if (!isMidGoingUp) {
            loadedFree(&newChildEntry.key);
            newChildEntry.key = malloc(sizeKeyToBeMovingAbove);
            memcpy(newChildEntry.key, indexNodeItr, sizeKeyToBeMovingAbove);
        }

        int belowChildPageNum = newChildEntry.pageNum;
        newChildEntry.pageNum = ixFileHandle.fileHandle.getNumberOfPages();

        // set split entry left pointer to right pointer of RecordToBeMovingAbove
        if (isMidGoingUp) {
            *((int *) splitNewPage) = belowChildPageNum;
        } else {
            void *leftPageNumSplitNodePtr = indexNodeItr + sizeKeyToBeMovingAbove;
            *((int *) splitNewPage) = *((int *) leftPageNumSplitNodePtr);
        }

        // common data to two nodes
        int start = sizeSoFar;
        int end = PAGE_SIZE - PAGE_META_SIZE - pageMeta.freeSpace;
        int sizeOfChunkToMove = end - start;

        // Insert keys from old leaf to new leaf and set meta (operations on new node)
        splitPageMeta.numSlots = pageMeta.numSlots - i - 1;
        int freeSpaceCalKeyGoingUpSize = 0;
        char *sourcePtrForNewChild;
        int splitNodeSize = 0;
        if (!isMidGoingUp) {
            freeSpaceCalKeyGoingUpSize = sizeRecordToBeMovingAbove - sizeof(int);
            sourcePtrForNewChild = pagePtr + sizeSoFar + sizeRecordToBeMovingAbove;
            splitNodeSize = sizeOfChunkToMove - sizeRecordToBeMovingAbove;
        } else {
            sourcePtrForNewChild = pagePtr + sizeSoFar;
            splitNodeSize = sizeOfChunkToMove;
        }
        splitPageMeta.freeSpace = pageMeta.freeSpace + sizeSoFar + freeSpaceCalKeyGoingUpSize;
        //Copy data into split node
        memmove((char *) splitNewPage + sizeof(int), sourcePtrForNewChild, splitNodeSize);
        setPageMetadata(splitNewPage, splitPageMeta);
        ixFileHandle.fileHandle.appendPage(splitNewPage);


        //old node
        memset(pagePtr + sizeSoFar, 0, splitNodeSize);
        pageMeta.numSlots = i;
        pageMeta.freeSpace = PAGE_SIZE - PAGE_META_SIZE - sizeSoFar;
        setPageMetadata(pagePtr, pageMeta);
        ixFileHandle.fileHandle.writePage(presentLeafPageNum, pagePtr);

        loadedFree(&splitNewPage);
        return PASS;
    }

    RC
    IndexManager::insertHelper(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid,
                               int insertInPageNum, childSplitInfo &newChildEntry) {
        //check node type
        void *presentPage = malloc(PAGE_SIZE);
        if (ixFileHandle.fileHandle.readPage(insertInPageNum, presentPage) == FAILED) {
            loadedFree(&presentPage);
            return FAILED;
        }
        IXPageMeta presentPageMeta;
        getPageMetadata(presentPage, presentPageMeta);
        IXNodeType nodeType = presentPageMeta.nodeType;

        if (nodeType == RootNode || nodeType == IndexNode) {
            //        Traverse through the index nodes, to get to the correct next sub tree node
            SubTreePageNum subTreeNode = getNextSubTreeNode(ixFileHandle, attribute, key, presentPage);
            if (subTreeNode == FAILED) {
                loadedFree(&presentPage);
                return FAILED;
            }
            //        Recursively get child page , till we get the leaf node.
            insertHelper(ixFileHandle, attribute, key, rid, (int) subTreeNode, newChildEntry);
            //        Come here after leaf node inserted with new data entry
            //        Check for split in leafpage, if so update index entries with info
            if (newChildEntry.pageNum == -1 && newChildEntry.key == NULL) {
                // NO split
                loadedFree(&presentPage);
                return PASS;
            }
            //        there is a split in leaf node
            // check if free space is sufficient
            if (insertIndexEntryInIndexNode((char *) presentPage, attribute, newChildEntry.key,
                                            newChildEntry.pageNum) == PASS) {
                ixFileHandle.fileHandle.writePage(insertInPageNum, presentPage);
                // update numchildentry to NULL and return
                newChildEntry.pageNum = -1;
                loadedFree(&newChildEntry.key);
                loadedFree(&presentPage);
                return PASS;
            }
            // index page needs to be split
            childSplitInfo preChildEntry;
            int prechildrecordSize = getDataEntrySize(attribute, newChildEntry.key);
            int prechildkeySize = prechildrecordSize - (sizeof(unsigned) + sizeof(unsigned short));
            preChildEntry.key = malloc(prechildkeySize);
            memcpy(preChildEntry.key, newChildEntry.key, prechildkeySize);
            preChildEntry.pageNum = newChildEntry.pageNum;
            if (splitIndexNode(ixFileHandle, (char *) presentPage, attribute, rid, newChildEntry, insertInPageNum) ==
                FAILED) {
                loadedFree(&presentPage);
                loadedFree(&preChildEntry.key);
                return FAILED;
            }
            // TODO:: if RC failed return FAILED
            if (compareKey(attribute, preChildEntry.key, newChildEntry.key) < 0) {
                insertIndexEntryInIndexNode((char *) presentPage, attribute, preChildEntry.key, preChildEntry.pageNum);
                ixFileHandle.fileHandle.writePage(insertInPageNum, presentPage);
            }// Equal case nothing to do.
            else if (compareKey(attribute, preChildEntry.key, newChildEntry.key) > 0) {
                void *splitLeafPtr = malloc(PAGE_SIZE);
                ixFileHandle.fileHandle.readPage(newChildEntry.pageNum, splitLeafPtr);
                insertIndexEntryInIndexNode((char *) splitLeafPtr, attribute, preChildEntry.key, preChildEntry.pageNum);
                ixFileHandle.fileHandle.writePage(newChildEntry.pageNum, splitLeafPtr);
                loadedFree(&splitLeafPtr);
            }
            loadedFree(&preChildEntry.key);
            // check if split node is root
            int rootPageNum;
            IXPageMeta rootPageMeta;
            getRootPageNum(ixFileHandle, rootPageNum);
            if (rootPageNum == insertInPageNum) {
                // we split the root node, so create a rootnode and add left pointer and right pointet to it
                //updating the new root entry
                void *rootPage = malloc(PAGE_SIZE);
                *((int *) rootPage) = insertInPageNum;
                initPageMetadata(rootPage, RootNode);
                if (insertIndexEntryInIndexNode((char *) rootPage, attribute, newChildEntry.key,
                                                newChildEntry.pageNum) == FAILED) {
                    loadedFree(&rootPage);
                    loadedFree(&presentPage);
                    return FAILED;
                }

                if (ixFileHandle.fileHandle.appendPage(rootPage) == FAILED) {
                    loadedFree(&rootPage);
                    loadedFree(&presentPage);
                    return FAILED;
                }

                //update dummy page
                setRootPageNum(ixFileHandle, ixFileHandle.fileHandle.getNumberOfPages() - 1);
                loadedFree(&rootPage);
                // update numchildentry to NULL and return
                newChildEntry.pageNum = -1;
                loadedFree(&newChildEntry.key);
                return PASS;
            }
            return PASS;
        } else if (nodeType == DummyNode) {
//            // std::cout << "Invalid node  type sent to helper" << std::endl;
            return FAILED;
        } else {
            //  If leaf node
            int RC = insertDataEntryInLeafNode((char *) presentPage, attribute, key, rid);
            std::stringstream out;
            // printBTree(ixFileHandle, attribute,out);
            //   printf("in insert helper entry end");
            if (RC != FAILED) {
                ixFileHandle.fileHandle.writePage(insertInPageNum, presentPage);
                loadedFree(&presentPage);
                return PASS;
            } else {
                // If free space in the page not sufficient then split the leaf
                if (splitLeafNode(ixFileHandle, (char *) presentPage, attribute, key, rid, newChildEntry,
                                  insertInPageNum) == -1) {
                    loadedFree(&presentPage);
                    return FAILED;
                }
                // once leaf node is split, decide which node to insert our data entry
                if (compareKey(attribute, key, newChildEntry.key) < 0) {
                    insertDataEntryInLeafNode((char *) presentPage, attribute, key, rid);
                    ixFileHandle.fileHandle.writePage(insertInPageNum, presentPage);
                } else {
                    void *splitLeafPtr = malloc(PAGE_SIZE);
                    ixFileHandle.fileHandle.readPage(newChildEntry.pageNum, splitLeafPtr);
                    insertDataEntryInLeafNode((char *) splitLeafPtr, attribute, key, rid);
                    ixFileHandle.fileHandle.writePage(newChildEntry.pageNum, splitLeafPtr);
                    loadedFree(&splitLeafPtr);
                }

                // if no root is created till this point create new root and return
                int rootPageNum;
                if (getRootPageNum(ixFileHandle, rootPageNum) == FAILED) {
                    loadedFree(&presentPage);
                    return FAILED;
                }

                if (rootPageNum == insertInPageNum) {
                    // we split the root node, so create a rootnode and add left pointer and right pointet to it
                    //Create root page which is just a leaf for the 1st entry.
                    //updating the new root entry
                    void *rootPage = malloc(PAGE_SIZE);
                    IXPageMeta rootPageMeta;
                    initPageMetadata(rootPage, RootNode);
                    *((int *) rootPage) = insertInPageNum;
                    if (insertIndexEntryInIndexNode((char *) rootPage, attribute, newChildEntry.key,
                                                    newChildEntry.pageNum) == FAILED) {
                        loadedFree(&rootPage);
                        loadedFree(&presentPage);
                        return FAILED;
                    }

                    if (ixFileHandle.fileHandle.appendPage(rootPage) == FAILED) {
                        loadedFree(&rootPage);
                        loadedFree(&presentPage);
                        return FAILED;
                    }

                    //update dummy page
                    setRootPageNum(ixFileHandle, ixFileHandle.fileHandle.getNumberOfPages() - 1);
                    loadedFree(&rootPage);
                    // update numchildentry to NULL and return
                    newChildEntry.pageNum = -1;
                    newChildEntry.key = NULL;
                }
            }
        }
        loadedFree(&presentPage);
        return PASS;
    }

    RC
    IndexManager::insertEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid) {
        // Setup Btree for the 1st time
        if (ixFileHandle.fileHandle.getNumberOfPages() == 0) {
            setupBTree(ixFileHandle, attribute, key, rid);
            return PASS;
        }

        //If root already present
        void *dummyPage = malloc(PAGE_SIZE);
        if (ixFileHandle.fileHandle.readPage(DUMMY_PAGE_FOR_ROOT, dummyPage) == FAILED) {
            loadedFree(&dummyPage);
            return FAILED;
        }
        int rootPageNum = *((int *) dummyPage);
        loadedFree(&dummyPage);

        // 'newchildentry is null initially, and null on retUTn unless child is split
        childSplitInfo newChildEntry;
        newChildEntry.pageNum = -1;
        newChildEntry.key = NULL;
        insertHelper(ixFileHandle, attribute, key, rid, rootPageNum, newChildEntry);
        if (newChildEntry.key != NULL) {
            loadedFree(&newChildEntry.key);
            return FAILED;
        }

        return PASS;
    }

    RC
    IndexManager::deleteEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid) {
        // printf("enter delete func");
        int rootPageNum = 0;
        getRootPageNum(ixFileHandle, rootPageNum);
        // printf("rootpage num %d", rootPageNum);
        int leafPageNum = -1;
        // get required leaf node if root node is not leaf node
        void *pagePtr = malloc(PAGE_SIZE);
        void *leafNodePtr = pagePtr;

        if (ixFileHandle.fileHandle.readPage(rootPageNum, pagePtr) == FAILED) {
            loadedFree(&pagePtr);
            return FAILED;
        }

        IXPageMeta pageMeta;
        int subTreePageNum = 0;
        getPageMetadata(pagePtr, pageMeta);
        if (pageMeta.nodeType == LeafNode) {
            leafPageNum = rootPageNum;
            if (deleteDataEntryInLeafNode((char *) leafNodePtr, attribute, key, rid) == FAILED) {
                loadedFree(&pagePtr);
                return FAILED;
            }
            if (ixFileHandle.fileHandle.writePage(leafPageNum, leafNodePtr) == FAILED) {
                loadedFree(&pagePtr);
                return FAILED;
            }
            loadedFree(&pagePtr);
            return PASS;
        }

        if (pageMeta.nodeType != LeafNode) {
            leafNodePtr = malloc(PAGE_SIZE);
            if (getLeafNodeForGivenKey(ixFileHandle, attribute, key, rootPageNum, leafPageNum, (char *) pagePtr) ==
                FAILED) {
                loadedFree(&pagePtr);
                loadedFree(&leafNodePtr);
                return FAILED;
            }
            if (ixFileHandle.fileHandle.readPage(leafPageNum, leafNodePtr) == FAILED) {
                loadedFree(&pagePtr);
                loadedFree(&leafNodePtr);
                return FAILED;
            }
        }
        //TODO :: delete in index node?
        // printf("%d leaf page num", leafPageNum);
        if (deleteDataEntryInLeafNode((char *) leafNodePtr, attribute, key, rid) == FAILED) {
            loadedFree(&pagePtr);
            loadedFree(&leafNodePtr);
            return FAILED;
        }
        if (ixFileHandle.fileHandle.writePage(leafPageNum, leafNodePtr) == FAILED) {
            loadedFree(&pagePtr);
            loadedFree(&leafNodePtr);
            return FAILED;
        }
        loadedFree(&pagePtr);
        loadedFree(&leafNodePtr);
        return PASS;
    }


    RC IndexManager::printBTree(IXFileHandle &ixFileHandle, const Attribute &attribute, std::ostream &out) const {
        //check if the file has tree at all
        if (ixFileHandle.fileHandle.getNumberOfPages() == 0) {
            //its empty tree
            // TODO::check testcase for expectation
            return -1;
        }
        if (ixFileHandle.fileHandle.getNumberOfPages() == 1) {
            //its empty tree
            // TODO::check testcase for expectation
            return -1;
        }
        // get rootpage to start the print
        int rootPageNum;
        void *dummyRootPage = malloc(PAGE_SIZE);
        ixFileHandle.fileHandle.readPage(DUMMY_PAGE_FOR_ROOT, dummyRootPage);
        // rootpage is stored as int in dummy page
        rootPageNum = *((int *) dummyRootPage);
        loadedFree(&dummyRootPage);
        // print from root
        // print keys
        // print children

        if (printBTreeIndices(ixFileHandle, attribute, rootPageNum, out) == -1) {
            return -1;
        }
        return 0;

    }

    RC IndexManager::printKey(const Attribute &attribute, std::ostream &out, const char *key) const {
        if (attribute.type == TypeInt) {
            int printKey;
            memcpy(&printKey, key, sizeof(int));
            out << printKey;
            // std::cout << printKey << std::endl;
        } else if (attribute.type == TypeReal) {
            float printKey;
            memcpy(&printKey, key, sizeof(float));
            out << printKey;
            // std::cout << printKey << std::endl;
        } else if (attribute.type == TypeVarChar) {
            int varCharLength = *((int *) key);
            char printKey[varCharLength + 1];
            memcpy(printKey, (char *) key + sizeof(int), varCharLength);
            printKey[varCharLength] = '\0';
            out << printKey;
            // std::cout << printKey << std::endl;
        }
        return PASS;
    }

    RC IndexManager::getKeyRID(const Attribute &attribute, char *dataEntryItr, void *key, RID &rid) const {
        if (attribute.type == TypeInt) {
            key = malloc(sizeof(int));
            *((int *) key) = *((int *) dataEntryItr);
            char *ridPageNumPtr = dataEntryItr + sizeof(int);
            rid.pageNum = *((unsigned *) ridPageNumPtr);
            char *ridSlotPtr = ridPageNumPtr + sizeof(unsigned);
            rid.slotNum = *((unsigned short *) ridSlotPtr);

        } else if (attribute.type == TypeReal) {
            key = malloc(sizeof(float));
            *((float *) key) = *((float *) dataEntryItr);
            char *ridPageNumPtr = dataEntryItr + sizeof(float);
            rid.pageNum = *((unsigned *) ridPageNumPtr);
            char *ridSlotPtr = ridPageNumPtr + sizeof(unsigned);
            rid.slotNum = *((unsigned short *) ridSlotPtr);

        } else {
            int varCharLength = *((int *) dataEntryItr);
            key = malloc(varCharLength + sizeof(int));
            char *varCharStartPtr = dataEntryItr;
            memcpy((char *) key, varCharStartPtr, varCharLength + sizeof(int));
            char *ridPageNumPtr = varCharStartPtr + varCharLength + sizeof(int);
            rid.pageNum = *((unsigned *) ridPageNumPtr);
            char *ridSlotPtr = ridPageNumPtr + sizeof(unsigned);
            rid.slotNum = *((unsigned short *) ridSlotPtr);
        }
        return PASS;

    }

    RC IndexManager::printBtreeLeafNode(IXFileHandle &ixFileHandle, const Attribute &attribute, std::ostream &out,
                                        const IXPageMeta pageMeta, char *dataEntryItr, void *key, RID &rid) const {
        // print leafpage
        out << "{\"keys\": [";
        // std::cout << "{\"keys\": [ \n";
        // group same key records in brackets(), intialise it
        int groupDataEntrySize = getDataEntrySize(attribute, dataEntryItr);
        int groupKeySize = groupDataEntrySize - (sizeof(unsigned) + sizeof(unsigned short));
        void *groupKey = malloc(groupKeySize);
        memcpy(groupKey, dataEntryItr, groupKeySize);
        int i = 0;
        //print key for first time
        out << "\"";
        printKey(attribute, out, (char *) groupKey);
        out << ":[";
        out << "(" << rid.pageNum << "," << rid.slotNum << ")";
        // std::cout << "(" << rid.pageNum << "," << rid.slotNum << ")";
        dataEntryItr += groupDataEntrySize;
        for (i = 1; i < pageMeta.numSlots; i++) {
            int dataEntrySize = getDataEntrySize(attribute, dataEntryItr);
            // get key, RID
            if (attribute.type == TypeInt) {
                key = malloc(sizeof(int));
                *((int *) key) = *((int *) dataEntryItr);
                char *ridPageNumPtr = dataEntryItr + sizeof(int);
                rid.pageNum = *((unsigned *) ridPageNumPtr);
                char *ridSlotPtr = ridPageNumPtr + sizeof(unsigned);
                rid.slotNum = *((unsigned short *) ridSlotPtr);

            } else if (attribute.type == TypeReal) {
                key = malloc(sizeof(float));
                *((float *) key) = *((float *) dataEntryItr);
                char *ridPageNumPtr = dataEntryItr + sizeof(float);
                rid.pageNum = *((unsigned *) ridPageNumPtr);
                char *ridSlotPtr = ridPageNumPtr + sizeof(unsigned);
                rid.slotNum = *((unsigned short *) ridSlotPtr);

            } else {
                int varCharLength = *((int *) dataEntryItr);
                key = malloc(varCharLength + sizeof(int));
                char *varCharStartPtr = dataEntryItr;
                memcpy((char *) key, varCharStartPtr, varCharLength + sizeof(int));
                char *ridPageNumPtr = varCharStartPtr + varCharLength + sizeof(int);
                rid.pageNum = *((unsigned *) ridPageNumPtr);
                char *ridSlotPtr = ridPageNumPtr + sizeof(unsigned);
                rid.slotNum = *((unsigned short *) ridSlotPtr);
            }
            // group similar keys in one bracket
            if (compareKey(attribute, groupKey, key) == 0) {
                out << ",(" << rid.pageNum << "," << rid.slotNum << ")";
                // std::cout << ",(" << rid.pageNum << "," << rid.slotNum << ")";
            } else {
                out << "]\",";
                // std::cout << "]\",";
                // update group key to current key
                groupKeySize = getDataEntrySize(attribute, dataEntryItr);
                groupKeySize -= (sizeof(unsigned) + sizeof(unsigned short));
                loadedFree(&groupKey);
                groupKey = malloc(groupKeySize);
                memcpy(groupKey, dataEntryItr, groupKeySize);
                out << "\"";
                printKey(attribute, out, (char *) key);
                out << ":[";
                out << "(" << rid.pageNum << "," << rid.slotNum << ")";
                // std::cout << "(" << rid.pageNum << "," << rid.slotNum << ")";
            }

            dataEntryItr += dataEntrySize;
            loadedFree(&key);
        }
        //TODO:: check if this closeing bracket to be put here
        out << "]";
        // std::cout << "]";
        out << "\"]} ";
        // std::cout << "\"]} ";
        loadedFree(&groupKey);
        return PASS;
    }

    RC IndexManager::printBTreeIndices(IXFileHandle &ixFileHandle, const Attribute &attribute, int indexPageNum,
                                       std::ostream &out) const {
        /*
         {"keys":["P"],
          "children":[
          {"keys":["C","G","M"],
              "children": [
              {"keys": ["A:[(1,1),(1,2)]","B:[(2,1),(2,2)]"]},
              {"keys": ["D:[(3,1),(3,2)]","E:[(4,1)]","F:[(5,1)]"]},
              {"keys": ["J:[(5,1),(5,2)]","K:[(6,1),(6,2)]","L:[(7,1)]"]},
              {"keys": ["N:[(8,1)]","O:[(9,1)]"]}
              ]},
          {"keys":["T","X"],
              "children": [
              {"keys": ["Q:[(10,1)]","R:[(11,1)]","S:[(12,1)]"]},
              {"keys": ["U:[(13,1)]","V:[(14,1)]"]},
              {"keys": ["Y:[(15,1)]","Z:[(16,1)]"]}
              ]}
          ]} // std::cout
          */
        void *mem = malloc(PAGE_SIZE);
        char *pagePtr = (char *) mem;
        IXPageMeta pageMeta;
        if (ixFileHandle.fileHandle.readPage(indexPageNum, pagePtr) == -1) {
            return -1;
        }
        char *freeSpacePtr = pagePtr + PAGE_SIZE - sizeof(int);
        pageMeta.freeSpace = *((int *) freeSpacePtr);
        char *numSlotsPtr = freeSpacePtr - sizeof(int);
        pageMeta.numSlots = *((int *) numSlotsPtr);
        //TODO:: does it look like a good design?
        if (pageMeta.numSlots == 0) {
            out << "{}";
            return PASS;
        }
        char *typePtr = numSlotsPtr - sizeof(char);
        pageMeta.nodeType = (IXNodeType) (*typePtr);
        void *key;
        RID rid;
        char *dataEntryItr = pagePtr;
        char *indexEntryItr = pagePtr;
        char *recordStartPointer = pagePtr;
        char *childrenNodeItr = pagePtr;

        // check if it is a leaf node
        if (pageMeta.nodeType == LeafNode) {
            printBtreeLeafNode(ixFileHandle, attribute, out, pageMeta, dataEntryItr, key, rid);

        } else {
            //TODO:: what if num slots are 0??
            //print keys
            out << "{\"keys\":[";
//            out << "{\n";
//            out<<"\"keys\":[";
            // std::cout << "{\"keys\":[";
            int i = 0;
            // start from the first key
            indexEntryItr += sizeof(int);
            for (i = 0; i < pageMeta.numSlots; i++) {
                int IndexEntrySize = getIndexEntrySize(attribute, indexEntryItr);
                if (attribute.type == TypeInt) {
                    key = malloc(IndexEntrySize - sizeof(int));
                    *((int *) key) = *((int *) indexEntryItr);
                } else if (attribute.type == TypeReal) {
                    key = malloc(IndexEntrySize - sizeof(int));
                    *((float *) key) = *((float *) indexEntryItr);
                } else if (attribute.type == TypeVarChar) {
                    int varCharLength = *((int *) indexEntryItr);
                    key = malloc(varCharLength + sizeof(int));
                    char *varCharStartPtr = indexEntryItr;
                    //TODO:: how to print keys for varchar
                    memcpy((char *) key, varCharStartPtr, varCharLength + sizeof(int));
                }
                out << "\"";
                printKey(attribute, out, (char *) key);
                out << "\"";
                if (i != pageMeta.numSlots - 1) {
                    out << ",";
                    // std::cout << ",";
                } else {
                    out << "], \n";
                    // std::cout << "], \n";
                }
                indexEntryItr += IndexEntrySize;
                loadedFree(&key);
            }
            // print children
            out << "\"children\": [ \n";
            // std::cout << "\"children\": [ \n";
            // print leftmost childnode first
            int leftPageNum = *(int *) childrenNodeItr;
            printBTreeIndices(ixFileHandle, attribute, leftPageNum, out);
            out << ", \n";
            // std::cout << ", \n";
            childrenNodeItr += sizeof(int);
            int childPageNum;
            //print rest of the child nodes
            for (i = 0; i < pageMeta.numSlots; i++) {
                int IndexEntrySize = getIndexEntrySize(attribute, childrenNodeItr);
                if (attribute.type == TypeInt) {
                    childPageNum = *((int *) childrenNodeItr + sizeof(int));
                } else if (attribute.type == TypeReal) {
                    childPageNum = *((int *) childrenNodeItr + sizeof(float));
                } else if (attribute.type == TypeVarChar) {
                    int varCharLength = *((int *) childrenNodeItr);
                    childPageNum = *(int *) (childrenNodeItr + sizeof(int) + varCharLength);
                }
                printBTreeIndices(ixFileHandle, attribute, childPageNum, out);
                if (i != pageMeta.numSlots - 1) {
                    out << ", \n";
                    // std::cout << ", \n";
                }
                childrenNodeItr += IndexEntrySize;
            }
            out << "]} ";
            // std::cout << "]} ";
        }
        loadedFree(&mem);
        return 0;
    }

    RC
    IndexManager::scan(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *lowKey, const void *highKey,
                       bool lowKeyInclusive, bool highKeyInclusive, IX_ScanIterator &ix_ScanIterator) {
        if (populateIterator(ixFileHandle, attribute, lowKey, highKey, lowKeyInclusive, highKeyInclusive,
                             ix_ScanIterator) == FAILED) {
            return FAILED;
        }
        return PASS;
    }

    RC IndexManager::getRequiredKeyRidPtrBasedOnKey(IXFileHandle &ixFileHandle, const Attribute &attribute,
                                                    const void *lowKey, const void *highKey, bool lowKeyInclusive,
                                                    bool highKeyInclusive, char *pagePtr, int &sizeSoFar, int &index) {

        IXPageMeta pageMeta;
        getPageMetadata(pagePtr, pageMeta);
        int requiredSlotPtr = 0;
        sizeSoFar = 0;
        index = 0;
        // there are no slots in this page
        // TODO :: if we want to include deleted entries empty pages to traverse further
        if (pageMeta.numSlots == 0) {
            return PASS;
        }

        char *dataEntryItr = pagePtr;

        for (index = 0; index < pageMeta.numSlots; index++) {
            //range = 0, we want lowest RID for that key ,range = 1 , we want highest RID in that key
            if (isKeyInValidRange(dataEntryItr, attribute, lowKey, highKey, lowKeyInclusive, highKeyInclusive)) {
                break;
            }

            int dataEntrySize = getDataEntrySize(attribute, dataEntryItr);
            dataEntryItr += dataEntrySize;
            sizeSoFar += dataEntrySize;
        }
        return PASS;
    }

    RC IndexManager::populateIterator(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *lowKey,
                                      const void *highKey, bool lowKeyInclusive, bool highKeyInclusive,
                                      IX_ScanIterator &ix_ScanIterator) {
        ix_ScanIterator.ixFileHandle = ixFileHandle;
        ix_ScanIterator.attribute = attribute;
        ix_ScanIterator.lowKey = lowKey;
        ix_ScanIterator.highKey = highKey;
        ix_ScanIterator.lowKeyInclusive = lowKeyInclusive;
        ix_ScanIterator.highKeyInclusive = highKeyInclusive;
        ix_ScanIterator.indexMgr = &IndexManager::instance();
        int rootPageNum = 0;
        int leafPageNum = 0;
        bool isHighkey = 0;
        ix_ScanIterator.currentIndexInActiveNode = 0;
        IXPageMeta pageMeta;

        if (getRootPageNum(ixFileHandle, rootPageNum) == FAILED) {
//            // std::cout << "Getting root page num failed" << std::endl;
            return FAILED;
        }

        ix_ScanIterator.activeLeafNodePtr = malloc(PAGE_SIZE);
        if (ixFileHandle.fileHandle.readPage(rootPageNum, ix_ScanIterator.activeLeafNodePtr) == FAILED) {
            loadedFree(&ix_ScanIterator.activeLeafNodePtr);
            return FAILED;
        }
        //printf(" in scan API, rootPagenUm %d", rootPageNum);
        ix_ScanIterator.indexMgr->getPageMetadata(ix_ScanIterator.activeLeafNodePtr, pageMeta);

        // get required leaf node as our initial seed to getNextEntry
        if (pageMeta.nodeType != LeafNode) {
            if (getLeafNodeForGivenKey(ixFileHandle, attribute, lowKey, rootPageNum, leafPageNum,
                                       (char *) ix_ScanIterator.activeLeafNodePtr) == FAILED) {
                loadedFree(&ix_ScanIterator.activeLeafNodePtr);
                return FAILED;
            }
            if (ixFileHandle.fileHandle.readPage(leafPageNum, ix_ScanIterator.activeLeafNodePtr) == FAILED) {
                loadedFree(&ix_ScanIterator.activeLeafNodePtr);
                return FAILED;
            }
        }

        // from the current leafNode, get the leftmost RID of the given Key
        if (getRequiredKeyRidPtrBasedOnKey(ixFileHandle, attribute, lowKey, highKey, lowKeyInclusive, highKeyInclusive,
                                           (char *) ix_ScanIterator.activeLeafNodePtr, ix_ScanIterator.sizeSoFar,
                                           ix_ScanIterator.currentIndexInActiveNode) == FAILED) {
            loadedFree(&ix_ScanIterator.activeLeafNodePtr);
            return FAILED;
        }

        //printf("sizeSoFar %d, currentIndexInActiveNode %d",ix_ScanIterator.sizeSoFar,ix_ScanIterator.currentIndexInActiveNode);
        return PASS;

    }

    RC IndexManager::isKeyInValidRange(void *currentKeyPtr, const Attribute &attribute, const void *lowKey,
                                       const void *highKey, bool lowKeyInclusive, bool highKeyInclusive) {
        //Low-key comparison
        bool lowComparison = false;
        int lc;
        if (lowKey) {
            lc = compareKey(attribute, lowKey, currentKeyPtr);
        } else {
            lc = -1;
        }
        if (lowKeyInclusive) {
            lowComparison = true ? (lc <= 0) : false;
        } else {
            lowComparison = true ? (lc < 0) : false;
        }

        //High-key comparison
        int hc;
        bool highComparison = false;
        if (highKey) {
            hc = compareKey(attribute, highKey, currentKeyPtr);
        } else {
            hc = 1;
        }
        if (highKeyInclusive) {
            highComparison = true ? (hc >= 0) : false;
        } else {
            highComparison = true ? (hc > 0) : false;
        }

        return lowComparison && highComparison;
    }

    IX_ScanIterator::IX_ScanIterator() {

    }

    IX_ScanIterator::~IX_ScanIterator() {
    }

    RC IX_ScanIterator::getNextEntry(RID &rid, void *key) {

        // get data entry
        IXPageMeta pageMeta;
        indexMgr->getPageMetadata(activeLeafNodePtr, pageMeta);
        char *ridPageNumPtr = (char *) activeLeafNodePtr;
        while (pageMeta.numSlots == 0 || currentIndexInActiveNode >= pageMeta.numSlots) {
            this->currentIndexInActiveNode = 0;
            sizeSoFar = 0;
            if (pageMeta.nextPagePtr == -5) {
                loadedFree(&activeLeafNodePtr);
                return IX_EOF;
            }
            int RC = ixFileHandle.fileHandle.readPage(pageMeta.nextPagePtr, activeLeafNodePtr);
            if (RC == FAILED) {
                loadedFree(&activeLeafNodePtr);
                return FAILED;
            }
            int RC1 = indexMgr->getPageMetadata(activeLeafNodePtr, pageMeta);
            if (RC1 == FAILED) {
                loadedFree(&activeLeafNodePtr);
                return IX_EOF;
            }
        }

        // // std::cout<<"currentIndexInActiveNode " <<currentIndexInActiveNode << " pageMeta.numSlots"<<pageMeta.numSlots;

        if (!indexMgr->isKeyInValidRange((char *) activeLeafNodePtr + sizeSoFar, attribute, lowKey, highKey,
                                         lowKeyInclusive, highKeyInclusive)) {
            loadedFree(&activeLeafNodePtr);
            return IX_EOF;
        }

        // // std::cout<<"currentIndexInActiveNode " <<currentIndexInActiveNode;

        char *recordStartPointer = (char *) activeLeafNodePtr + sizeSoFar;
        int dataEntrySize = indexMgr->getDataEntrySize(attribute, recordStartPointer);
        // // std::cout<<"sizeSoFar " <<sizeSoFar << " dataEntrySize" << dataEntrySize ;
        if (attribute.type == TypeInt) {
            *((int *) key) = *((int *) recordStartPointer);
            ridPageNumPtr = recordStartPointer + sizeof(int);
        } else if (attribute.type == TypeReal) {
            *((float *) key) = *((float *) recordStartPointer);
            ridPageNumPtr = recordStartPointer + sizeof(float);
        } else {
            int varCharLength = *((int *) recordStartPointer);
//            key = malloc(varCharLength + sizeof(int));
            char *varCharStartPtr = recordStartPointer;
            memcpy((char *) key, varCharStartPtr, varCharLength + sizeof(int));
            ridPageNumPtr = recordStartPointer + varCharLength + sizeof(int);
        }
        //// std::cout<<"key"<<*(int*)key;
        rid.pageNum = *((unsigned *) ridPageNumPtr);
        char *ridSlotPtr = ridPageNumPtr + sizeof(unsigned);
        rid.slotNum = *((unsigned short *) ridSlotPtr);
        //printf("rid.pageNum  %d ,rid.slotNum %d ",rid.pageNum,rid.slotNum );
        currentIndexInActiveNode += 1;
        sizeSoFar += dataEntrySize;

        return PASS;
    }

    RC IX_ScanIterator::close() {
        if (activeLeafNodePtr) {
            loadedFree(&activeLeafNodePtr);
        }
        this->lowKey = NULL;
        this->highKey = NULL;
        this->lowKeyInclusive = false;
        this->highKeyInclusive = false;
        this->preLeafNodePtr = NULL;
        this->leftMostKeyRidPtr = NULL;
        this->sizeSoFar = 0;
        this->currentIndexInActiveNode = 0;
        this->indexMgr = NULL;
        return PASS;
    }

    IXFileHandle::IXFileHandle() {
        ixReadPageCounter = 0;
        ixWritePageCounter = 0;
        ixAppendPageCounter = 0;
    }

    IXFileHandle::~IXFileHandle() {
    }

    RC
    IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
        if (fileHandle.activeFile != NULL) {
            fileHandle.collectCounterValues(ixReadPageCounter, ixWritePageCounter, ixAppendPageCounter);
            readPageCount = ixReadPageCounter;
            writePageCount = ixWritePageCounter;
            appendPageCount = ixAppendPageCounter;
            return 0;
        }

        return -1;
    }

} // namespace PeterDB