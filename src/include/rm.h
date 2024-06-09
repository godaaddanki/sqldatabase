#ifndef _rm_h_
#define _rm_h_

#include <string>
#include <vector>
#include <unistd.h>
#include "src/include/rbfm.h"
#include "src/include/ix.h"
namespace PeterDB {
#define RM_EOF (-1)  // end of a scan operator

    // RM_ScanIterator is an iterator to go through tuples
    class RM_ScanIterator {
    public:
        RM_ScanIterator();

        ~RM_ScanIterator();

        // "data" follows the same format as RelationManager::insertTuple()
        RC getNextTuple(RID &rid, void *data);

        RC close();

        FileHandle tableFileHandle;
        RBFM_ScanIterator rbfManagerScanIterator;
    };

    // RM_IndexScanIterator is an iterator to go through index entries
    class RM_IndexScanIterator {
    public:
        IX_ScanIterator scanIterator;
        RM_IndexScanIterator(){};    // Constructor
        ~RM_IndexScanIterator(){};    // Destructor
        IXFileHandle ixFileHandle;
        // "key" follows the same format as in IndexManager::insertEntry()
        RC getNextEntry(RID &rid, void *key);    // Get next matching entry
        RC close();                              // Terminate index scan
    };

    // Relation Manager
    class RelationManager {
    public:
        static RelationManager &instance();

        std::string tablesTable = "Tables";
        std::string columnsTable = "Columns";

        std::vector<Attribute> tablesTableRecordDescriptor;
        std::vector<Attribute> columnTableRecordDescriptor;

#define BUFFER 10
#define TABLES_RECORD_MAX_SIZE sizeof(int)+sizeof(int)+50+sizeof(int)+50+BUFFER
#define COLUMNS_RECORD_MAX_SIZE sizeof(int)+sizeof(int)+50+sizeof(int)+sizeof(int)+sizeof(int)+BUFFER

        RC createTablesTableRecordDescriptor(std::vector<Attribute> &tablesTableRecordDescriptor);

        RC createColumnsTableRecordDescriptor(std::vector<Attribute> &columnTableRecordDescriptor);

        void createTablesTableRecord(const std::vector<Attribute> &tablesTableRecordDescriptor, void *tablesTableRecord,
                                     int &tableIndex, const std::string &tableName);

        void createColumnsTableRecord(const std::vector<Attribute> &columnTableRecordDescriptor,
                                      const std::vector<Attribute> &currentRecordDescriptor,
                                      void *columnsTableRecord,
                                      int &tableIndex, const std::string &tableName, int &position);

        RC insertRecordInTablesTable(const std::string &tableName, int &tableIndex);

        RC
        insertRecordInColumnsTable(const std::vector<Attribute> &currentRecordDescriptor, const std::string &tableName,
                                   int &tableIndex);

        RC createCatalog();

        RC deleteCatalog();

        RC createTable(const std::string &tableName, const std::vector<Attribute> &attrs);

        RC getNextTableIndex(int &tableIndex);

        RC getRequiredTablesTableIdAndRid(const std::string &tableName, int &tableIndex, RID &tablesTableRID);

        RC getRequiredColumnsTableRid(const std::string &tableName, const int &tableIndex,
                                      std::vector<RID> &columnsTableRID);

        RC deleteTable(const std::string &tableName);

        RC getAttributes(const std::string &tableName, std::vector<Attribute> &attrs);

        RC insertTuple(const std::string &tableName, const void *data, RID &rid);

        RC deleteTuple(const std::string &tableName, const RID &rid);

        RC updateTuple(const std::string &tableName, const void *data, const RID &rid);

        RC readTuple(const std::string &tableName, const RID &rid, void *data);

        // Print a tuple that is passed to this utility method.
        // The format is the same as printRecord().
        RC printTuple(const std::vector<Attribute> &attrs, const void *data, std::ostream &out);

        RC readAttribute(const std::string &tableName, const RID &rid, const std::string &attributeName, void *data);

        RC checkCatalogExists();

        // Scan returns an iterator to allow the caller to go through the results one by one.
        // Do not store entire results in the scan iterator.
        RC scan(const std::string &tableName,
                const std::string &conditionAttribute,
                const CompOp compOp,                  // comparison type such as "<" and "="
                const void *value,                    // used in the comparison
                const std::vector<std::string> &attributeNames, // a list of projected attributes
                RM_ScanIterator &rm_ScanIterator);

        // Extra credit work (10 points)
        RC addAttribute(const std::string &tableName, const Attribute &attr);

        RC dropAttribute(const std::string &tableName, const std::string &attributeName);

        // QE IX related
        RC createIndex(const std::string &tableName, const std::string &attributeName);

        RC destroyIndex(const std::string &tableName, const std::string &attributeName);
        std::string  checkIndexExists(const std::string &indexAttribute,const std::string &tableNameToCheck,bool &isIndexPresent);

        // indexScan returns an iterator to allow the caller to go through qualified entries in index
        RC indexScan(const std::string &tableName,
                     const std::string &attributeName,
                     const void *lowKey,
                     const void *highKey,
                     bool lowKeyInclusive,
                     bool highKeyInclusive,
                     RM_IndexScanIterator &rm_IndexScanIterator);
        bool checkNullIndicator(const char *byte);
        bool getAttributeFromAttName(const std::string &tableName, Attribute &requiredAttribute, const std::string &attributeName);

    protected:
        RelationManager();                                                  // Prevent construction
        ~RelationManager();                                                 // Prevent unwanted destruction
        RelationManager(const RelationManager &);                           // Prevent construction by copying
        RelationManager &operator=(const RelationManager &);                // Prevent assignment

    };

} // namespace PeterDB

#endif // _rm_h_