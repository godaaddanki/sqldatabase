#ifndef _qe_h_
#define _qe_h_

#include <vector>
#include <string>
#include <unordered_map>
#include "rm.h"
#include "ix.h"

namespace PeterDB {
    void mergeRecordDescriptors(std::vector<Attribute> &leftRecordDescriptor, std::vector<Attribute> &rightRecordDescriptor, std::vector<Attribute> &out);
    RC GetAttributeValueFromData(const std::vector<Attribute> recordDescriptor,
                                 const std::string &attributeName, void *inputData, void *output);

    RC mergeRecords(std::vector<Attribute> &leftRecordDescriptor, std::vector<Attribute> &rightRecordDescriptor, void *leftRecord, void *rightRecord,
                      void *resultRecord);

    bool getAttributeFromAttName( Attribute &requiredAttribute,
                                 const std::string &attributeName,const std::vector<Attribute> tableRecordDescriptor);
    bool checkBitSet(int index, char *inputRecordBitMap);
    RC getAttributeValueFromData(const std::vector<Attribute> recordDescriptor,const std::string &attributeName, void *inputData,void* lhsValue);

#define QE_EOF (-1)  // end of the index scan
    typedef enum AggregateOp {
        MIN = 0, MAX, COUNT, SUM, AVG
    } AggregateOp;

    // The following functions use the following
    // format for the passed data.
    //    For INT and REAL: use 4 bytes
    //    For VARCHAR: use 4 bytes for the length followed by the characters

    typedef struct Value {
        AttrType type;          // type of value
        void *data;             // value
    } Value;

    typedef struct Condition {
        std::string lhsAttr;        // left-hand side attribute
        CompOp op;                  // comparison operator
        bool bRhsIsAttr;            // TRUE if right-hand side is an attribute and not a value; FALSE, otherwise.
        std::string rhsAttr;        // right-hand side attribute if bRhsIsAttr = TRUE
        Value rhsValue;             // right-hand side value if bRhsIsAttr = FALSE
    } Condition;
    bool handleNullMembers(char* lhsRecord, char *rhsRecord,Condition &condition);

    RC floatCompare(float actual, float expected);
    bool recordCompare(const void *lhsRecord, const void *rhsRecord,const AttrType attType,Condition &condition );
    bool handleEmptyExpectedRecordComparision(void *Value, Condition &condition);
    class Iterator {
        // All the relational operators and access methods are iterators.
    public:
        virtual RC getNextTuple(void *data) = 0;

        virtual RC getAttributes(std::vector<Attribute> &attrs) const = 0;


        virtual ~Iterator() = default;
    };

    class TableScan : public Iterator {
        // A wrapper inheriting Iterator over RM_ScanIterator
    private:
        RelationManager &rm;
        RM_ScanIterator iter;
        std::string tableName;
        std::vector<Attribute> attrs;
        std::vector<std::string> attrNames;
        RID rid;
    public:
        TableScan(RelationManager &rm, const std::string &tableName, const char *alias = NULL) : rm(rm) {
            //Set members
            this->tableName = tableName;

            // Get Attributes from RM
            rm.getAttributes(tableName, attrs);

            // Get Attribute Names from RM
            for (const Attribute &attr: attrs) {
                // convert to char *
                attrNames.push_back(attr.name);
            }

            // Call RM scan to get an iterator
            rm.scan(tableName, "", NO_OP, NULL, attrNames, iter);

            // Set alias
            if (alias) this->tableName = alias;
        };

        // Start a new iterator given the new compOp and value
        void setIterator() {
            iter.close();
            rm.scan(tableName, "", NO_OP, NULL, attrNames, iter);
        };

        RC getNextTuple(void *data) override {
            return iter.getNextTuple(rid, data);
        };

        RC getAttributes(std::vector<Attribute> &attributes) const override {
            attributes.clear();
            attributes = this->attrs;

            // For attribute in std::vector<Attribute>, name it as rel.attr
            for (Attribute &attribute: attributes) {
                attribute.name = tableName + "." + attribute.name;
            }
            return PASS;
        };

        ~TableScan() override {
            iter.close();
        };
    };

    class IndexScan : public Iterator {
        // A wrapper inheriting Iterator over IX_IndexScan
    private:
        RelationManager &rm;
        RM_IndexScanIterator iter;
        std::string tableName;
        std::string attrName;
        std::vector<Attribute> attrs;
        char key[PAGE_SIZE];
        RID rid;
    public:
        IndexScan(RelationManager &rm, const std::string &tableName, const std::string &attrName,
                  const char *alias = NULL) : rm(rm) {
            // Set members
            this->tableName = tableName;
            this->attrName = attrName;

            // Get Attributes from RM
            rm.getAttributes(tableName, attrs);

            // Call rm indexScan to get iterator
            rm.indexScan(tableName, attrName, NULL, NULL, true, true, iter);

            // Set alias
            if (alias) this->tableName = alias;
        };

        // Start a new iterator given the new key range
        void setIterator(void *lowKey, void *highKey, bool lowKeyInclusive, bool highKeyInclusive) {
            iter.close();
            rm.indexScan(tableName, attrName, lowKey, highKey, lowKeyInclusive, highKeyInclusive, iter);
        };

        RC getNextTuple(void *data) override {
            RC rc = iter.getNextEntry(rid, key);
            if (rc == 0) {
                rc = rm.readTuple(tableName, rid, data);
            }
            return rc;
        };

        RC getAttributes(std::vector<Attribute> &attributes) const override {
            attributes.clear();
            attributes = this->attrs;


            // For attribute in std::vector<Attribute>, name it as rel.attr
            for (Attribute &attribute: attributes) {
                attribute.name = tableName + "." + attribute.name;
            }
            return  PASS;
        };

        ~IndexScan() override {
            iter.close();
        };
    };

    class  Filter : public Iterator {
        // Filter operator
        Iterator *input;
        Condition condition;
        std::vector<Attribute> filterRecordDescriptor;
    public:
        Filter(Iterator *input,               // Iterator of input R
               const Condition &condition     // Selection condition
        );

        ~Filter() override;

        RC getNextTuple(void *data) override;

        // For attribute in std::vector<Attribute>, name it as rel.attr
        RC getAttributes(std::vector<Attribute> &attrs) const override;

//        bool isConditionMet(void *lhs, void *rhs, AttrType &attributeType);
        bool checkNullIndicator(const char *byte);
        void setupFilter(const Condition &condition);

    };

    class Project : public Iterator {
        // Projection operator
        Iterator *input;
        std::vector<std::string> attrNames;
        std::vector<Attribute> recordDescriptor;
        std::vector<Attribute> projectRecordDescriptor;
        std::vector<int> filteredNeededAttributeList;
    public:
        Project(Iterator *input,                                // Iterator of input R
                const std::vector<std::string> &attrNames);     // std::vector containing attribute names
        ~Project() override;

        RC getNextTuple(void *data) override;

        // For attribute in std::vector<Attribute>, name it as rel.attr
        RC getAttributes(std::vector<Attribute> &attrs) const override;

        void setupProject();
        RC prepareProjectedRecord(void *inputData,void* projectedRecord);

    };

    class BNLJoin : public Iterator {
        Iterator *leftIn;
        TableScan *rightIn;
        Condition condition;
        unsigned  int numPages;
        bool dataLeftInRightTable;
        bool dataLeftInLeftTable;
        bool dataPresentInHashMapBlock;
        std::vector<Attribute> leftTableRecordDescriptor;
        std::vector<Attribute> rightTableRecordDescriptor;
        std::vector<Attribute> joinedTableRecordDescriptor;
        Attribute leftConditionAttribute;
        Attribute rightConditionAttribute;
        void* rightRecord ;
        void* leftRecord ;
        bool currentVectorForRightExhaused;
        void* rightAttributeValue;
        std::unordered_map<int,std::vector<void*>> intLeftBlockHashMap;
        std::unordered_map<float,std::vector<void*>> floatLeftBlockHashMap;
        std::unordered_map<std::string ,std::vector<void*>> stringLeftBlockHashMap;
        int presentIndexWithGivenKey;


    public:
        BNLJoin(Iterator *leftIn,            // Iterator of input R
                TableScan *rightIn,           // TableScan Iterator of input S
                const Condition &condition,   // Join condition
                const unsigned numPages       // # of pages that can be loaded into memory,
                //   i.e., memory block size (decided by the optimizer)
        );
        void setUp();
        ~BNLJoin() override;
        RC clearHashMap();
        RC getNextTuple(void *data) override;
        RC prepareLeftBlock(const unsigned numPages);
        int getRecordSize(void* record);
        // For attribute in std::vector<Attribute>, name it as rel.attr
        RC getAttributes(std::vector<Attribute> &attrs) const override;
        bool searchForRightRecordInHashMap(void** leftRecordPtr);
    };

    class INLJoin : public Iterator {
        // Index nested-loop join operator
        Iterator *leftIn;
        IndexScan *rightIn;
        Condition condition;
        std::vector<Attribute> leftTableRecordDescriptor;
        std::vector<Attribute> rightTableRecordDescriptor;
        std::vector<Attribute> joinedTableRecordDescriptor;
        Attribute leftConditionAttribute;
        Attribute rightCinditionAttribute;
        bool nodatainLeftTable;
        bool nodatainRightTable ;
        void* leftRecord ;
        void* leftExtractedValue ;
        void* activerightRecord;
    public:
        INLJoin(Iterator *leftIn,           // Iterator of input R
                IndexScan *rightIn,          // IndexScan Iterator of input S
                const Condition &condition   // Join condition
        );

        ~INLJoin() override;

        RC getNextTuple(void *data) override;

        // For attribute in std::vector<Attribute>, name it as rel.attr
        RC getAttributes(std::vector<Attribute> &attrs) const override;
        void setUp();
    };

    // 10 extra-credit points
    class GHJoin : public Iterator {
        // Grace hash join operator
    public:
        GHJoin(Iterator *leftIn,               // Iterator of input R
               Iterator *rightIn,               // Iterator of input S
               const Condition &condition,      // Join condition (CompOp is always EQ)
               const unsigned numPartitions     // # of partitions for each relation (decided by the optimizer)
        );

        ~GHJoin() override;

        RC getNextTuple(void *data) override;

        // For attribute in std::vector<Attribute>, name it as rel.attr
        RC getAttributes(std::vector<Attribute> &attrs) const override;
    };

    class Aggregate : public Iterator {
        // Aggregation operator
    public:
        // Mandatory
        // Basic aggregation
        Aggregate(Iterator *input,          // Iterator of input R
                  const Attribute &aggAttr,        // The attribute over which we are computing an aggregate
                  AggregateOp op            // Aggregate operation
        );

        // Optional for everyone: 5 extra-credit points
        // Group-based hash aggregation
        Aggregate(Iterator *input,             // Iterator of input R
                  const Attribute &aggAttr,           // The attribute over which we are computing an aggregate
                  const Attribute &groupAttr,         // The attribute over which we are grouping the tuples
                  AggregateOp op              // Aggregate operation
        );

        ~Aggregate() override;

        RC getNextTuple(void *data) override;

        // Please name the output attribute as aggregateOp(aggAttr)
        // E.g. Relation=rel, attribute=attr, aggregateOp=MAX
        // output attrName = "MAX(rel.attr)"
        RC getAttributes(std::vector<Attribute> &attrs) const override;
        void setupAggregate();
        Iterator *input;
        Attribute aggAttr;
        AggregateOp op;
        bool isEndReached;
        std::vector<Attribute> aggregateRecordDescriptor;
        RC aggregateGetAttributeValueFromData(const std::vector<Attribute> recordDescriptor, const std::string &attributeName,
                                                         void *inputData, void *output);

    };
} // namespace PeterDB

#endif // _qe_h_