#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>

#include "pfm.h"
#include "rbfm.h" // for some type declarations only, e.g., RID and Attribute

# define IX_EOF (-1)  // end of the index scan

namespace PeterDB {

    //Node type
    typedef enum {
        DummyNode = 0, RootNode, IndexNode, LeafNode
    } IXNodeType;

    // Page meta
    typedef struct {
        int numSlots;
        int freeSpace;
        IXNodeType nodeType;
        int prevPagePtr;
        int nextPagePtr;
    } IXPageMeta;

    //Info from child to parent node when child splits.
    typedef struct {
        int pageNum;
        void *key;
    } childSplitInfo;

    //Index node record struct
    typedef struct {
        int leftPtr;
        int rightPtr;
        void *key;
    } indexNodeRecord;

    typedef int SubTreePageNum;

    class IX_ScanIterator;

    class IXFileHandle;

    class IndexManager {

    #define DUMMY_PAGE_FOR_ROOT  0
    #define isLeafPageByte 0
    #define isLeafPageByteSize sizeof(char)
    #define PAGE_META_SIZE  (4*(sizeof(int)) + sizeof(char))
    #define NON_LEAF_META_SIZE  (5*(sizeof(int)) + sizeof(char))

    public:
        static IndexManager &instance();

        // Create an index file.
        RC createFile(const std::string &fileName);

        // Delete an index file.
        RC destroyFile(const std::string &fileName);

        // Open an index and return an ixFileHandle.
        RC openFile(const std::string &fileName, IXFileHandle &ixFileHandle);

        // Close an ixFileHandle for an index.
        RC closeFile(IXFileHandle &ixFileHandle);

        // Insert an entry into the given index that is indicated by the given ixFileHandle.
        RC insertEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Delete an entry from the given index that is indicated by the given ixFileHandle.
        RC deleteEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Initialize and IX_ScanIterator to support a range search
        RC scan(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *lowKey, const void *highKey,
                bool lowKeyInclusive, bool highKeyInclusive, IX_ScanIterator &ix_ScanIterator);

        // Print the B+ tree in pre-order (in a JSON record format)
        RC printBTree(IXFileHandle &ixFileHandle, const Attribute &attribute, std::ostream &out) const;

        RC printBTreeIndices(IXFileHandle &ixFileHandle, const Attribute &attribute, int indexPageNum,
                             std::ostream &out) const;

        RC initPageMetadata(void *pagePtr, IXNodeType nodeType);

        RC setPageMetadata(void *pagePtr, IXPageMeta &pageMeta);

        RC getPageMetadata(const void *pagePtr, IXPageMeta &pageMeta);

        RC getRootPageNum(IXFileHandle &ixFileHandle, int &rootPageNum);

        RC setRootPageNum(IXFileHandle &ixFileHandle, int rootPageNum);

        RC
        getLeafNodeForGivenKey(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key,
                               int rootPageNum, int &leafPageNum, char *pagePtr);

        int getDataEntrySize(const Attribute &attribute, const void *key) const;

        int getIndexEntrySize(const Attribute &attribute, const void *key) const;

        RC insertDataEntryInLeafNode(char *pagePtr, const Attribute &attribute, const void *key, const RID &rid);

        int floatCompare(float inputKey, float keyInNode) const;

        int compareKey(const Attribute &attribute, const void *inputKey, const void *keyInNode) const;

        RC deleteDataEntryInLeafNode(char *pagePtr, const Attribute &attribute, const void *key, const RID &rid);

        RC setupBTree(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid);

        RC insertIndexEntryInIndexNode(char *pagePtr, const Attribute &attribute, const void *key, int childPageNum);

        RC deleteIndexEntryInIndexNode(char *pagePtr, const Attribute &attribute, const void *key, int childPageNum);

        RC insertHelper(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid,
                        int insertInPageNum, childSplitInfo &newChildEntry);

        RC splitLeafNode(IXFileHandle &ixFileHandle, char *pagePtr, const Attribute &attribute, const void *key,
                         const RID &rid, childSplitInfo &newChildEntry, const int presentLeafPageNum);

        RC getNextSubTreeNode(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key,
                              const void *presentPage);

        RC splitIndexNode(IXFileHandle &ixFileHandle, char *pagePtr, const Attribute &attribute,
                          const RID &rid, childSplitInfo &newChildEntry, const int presentLeafPageNum);

        RC populateIterator(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *lowKey,
                            const void *highKey, bool lowKeyInclusive, bool highKeyInclusive,
                            IX_ScanIterator &ix_ScanIterator);
        RC printBtreeLeafNode(IXFileHandle &ixFileHandle, const Attribute &attribute, std::ostream &out, const IXPageMeta pageMeta,char* dataEntryItr,
                                void* key, RID &rid) const;

        RC getRequiredKeyRidPtrBasedOnKey(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *lowKey,
                                          const void *highKey, bool lowKeyInclusive, bool highKeyInclusive, char* pagePtr , int & sizeSoFar, int &index );
        RC isKeyInValidRange(void* currentKeyPtr,const Attribute &attribute, const void *lowKey, const void *highKey,
                             bool lowKeyInclusive, bool highKeyInclusive );
        int adjustSizeBasedOnSameKey(const void *keyToBeInserted, const void *midKey, const Attribute &attribute,
                                               char *dataEntryItr, IXPageMeta &pageMeta, int& index);

        RC printKey(const Attribute &attribute, std::ostream &out,const char *key) const;

        RC getKeyRID(const Attribute &attribute, char *dataEntryItr, void *key,RID &rid) const;


    protected:
        IndexManager() = default;                                                   // Prevent construction
        ~IndexManager() = default;                                                  // Prevent unwanted destruction
        IndexManager(const IndexManager &) = default;                               // Prevent construction by copying
        IndexManager &operator=(const IndexManager &) = default;                    // Prevent assignment

    };

    class IXFileHandle {
    public:
        // variables to keep counter for each operation
        unsigned ixReadPageCounter;
        unsigned ixWritePageCounter;
        unsigned ixAppendPageCounter;

        FileHandle fileHandle;

        // Constructor
        IXFileHandle();

        // Destructor
        ~IXFileHandle();

        // Put the current counter values of associated PF FileHandles into variables
        RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);
    };

    class IX_ScanIterator {
    public:
        //scan iterator variables
        IXFileHandle ixFileHandle;
        Attribute attribute;
        const void *lowKey;
        const void *highKey;
        bool lowKeyInclusive;
        bool highKeyInclusive;
        void* activeLeafNodePtr;
        void *preLeafNodePtr;
        void *leftMostKeyRidPtr;
        int sizeSoFar;
        int currentIndexInActiveNode;
        IndexManager *indexMgr;



        // Constructor
        IX_ScanIterator();

        // Destructor
        ~IX_ScanIterator();

        // Get next matching entry
        RC getNextEntry(RID &rid, void *key);

        // Terminate index scan
        RC close();

    };


}// namespace PeterDB
#endif // _ix_h_
