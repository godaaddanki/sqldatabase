#include <sys/stat.h>
#include "src/include/rm.h"
#include <string>


namespace PeterDB {
    RelationManager &RelationManager::instance() {
        static RelationManager _relation_manager = RelationManager();
        return _relation_manager;
    }

    RelationManager::RelationManager() {
        createTablesTableRecordDescriptor(tablesTableRecordDescriptor);
        createColumnsTableRecordDescriptor(columnTableRecordDescriptor);
    }

    RelationManager::~RelationManager() = default;

    RelationManager::RelationManager(const RelationManager &) = default;

    RelationManager &RelationManager::operator=(const RelationManager &) = default;

    static RecordBasedFileManager *rbfManagerPtr = &RecordBasedFileManager::instance();

    static IndexManager *indexMangerPtr = &IndexManager::instance();

    RC RelationManager::createTablesTableRecordDescriptor(std::vector<Attribute> &tablesTableRecordDescriptor) {
        Attribute attr;

        attr.name = "table-id";
        attr.type = PeterDB::TypeInt;
        attr.length = (PeterDB::AttrLength) 4;
        tablesTableRecordDescriptor.push_back(attr);

        attr.name = "table-name";
        attr.type = PeterDB::TypeVarChar;
        attr.length = (PeterDB::AttrLength) 50;
        tablesTableRecordDescriptor.push_back(attr);

        attr.name = "file-name";
        attr.type = PeterDB::TypeVarChar;
        attr.length = (PeterDB::AttrLength) 50;
        tablesTableRecordDescriptor.push_back(attr);

        return 0;

    }

    RC RelationManager::createColumnsTableRecordDescriptor(std::vector<Attribute> &columnTableRecordDescriptor) {
        Attribute attr;

        attr.name = "table-id";
        attr.type = PeterDB::TypeInt;
        attr.length = (PeterDB::AttrLength) 4;
        columnTableRecordDescriptor.push_back(attr);

        attr.name = "column-name";
        attr.type = PeterDB::TypeVarChar;
        attr.length = (PeterDB::AttrLength) 50;
        columnTableRecordDescriptor.push_back(attr);

        attr.name = "column-type";
        attr.type = PeterDB::TypeInt;
        attr.length = (PeterDB::AttrLength) 4;
        columnTableRecordDescriptor.push_back(attr);

        attr.name = "column-length";
        attr.type = PeterDB::TypeInt;
        attr.length = (PeterDB::AttrLength) 4;
        columnTableRecordDescriptor.push_back(attr);

        attr.name = "column-position";
        attr.type = PeterDB::TypeInt;
        attr.length = (PeterDB::AttrLength) 4;
        columnTableRecordDescriptor.push_back(attr);

        return 0;

    }

    void RelationManager::createTablesTableRecord(const std::vector<Attribute> &tablesTableRecordDescriptor,
                                                  void *tablesTableRecord, int &tableIndex,
                                                  const std::string &tableName) {
        int numFieldsInRecord = tablesTableRecordDescriptor.size();
        //printf(" num feilds in record %d, ",numFieldsInRecord);
        // fixed 3 fileds ,so 1 byte is enough
        char *tablesTableRecordItr = (char *) tablesTableRecord;
        char *tablesTableRecordData = tablesTableRecordItr + sizeof(char);

        // set nullfields to 0
        memset(tablesTableRecordItr, 0, sizeof(char));

        //copy data
        for (int index = 0; index < numFieldsInRecord; index++) {
            //std::cout<<tablesTableRecordDescriptor[index].name<<std::endl;
            if (tablesTableRecordDescriptor[index].name == "table-id") {
                memcpy(tablesTableRecordData, &tableIndex, sizeof(int));
                //std::cout << "ID :"<<(*(int*)tablesTableRecordData)<<" ";
                tablesTableRecordData += sizeof(int);
            } else if (tablesTableRecordDescriptor[index].name == "table-name") {
                int varCharLength = tableName.length();
                //std::cout << "file name length: "<<varCharLength<<" ";
                memcpy(tablesTableRecordData, &varCharLength, sizeof(int));
                tablesTableRecordData += sizeof(int);
                memcpy(tablesTableRecordData, tableName.c_str(), varCharLength);
                //std::cout << "Name letter: "<<(*(char*)(tablesTableRecordData+10))<<" ";
                tablesTableRecordData += varCharLength;
            } else if (tablesTableRecordDescriptor[index].name == "file-name") {
                int varCharLength = tableName.length();
                //std::cout << "file name length: "<<varCharLength<<" ";
                memcpy(tablesTableRecordData, &varCharLength, sizeof(int));
                tablesTableRecordData += sizeof(int);
                memcpy(tablesTableRecordData, tableName.c_str(), varCharLength);
                //std::cout << " file name letter:"<<(*(char*)(tablesTableRecordData+10))<<std::endl;
                tablesTableRecordData += varCharLength;
            }
        }
    }

    void RelationManager::createColumnsTableRecord(const std::vector<Attribute> &columnTableRecordDescriptor,
                                                   const std::vector<Attribute> &currentRecordDescriptor,
                                                   void *columnsTableRecord, int &tableIndex,
                                                   const std::string &tableName, int &position) {
        int numFieldsInRecord = columnTableRecordDescriptor.size();

        char *columnsTableRecordItr = (char *) columnsTableRecord;
        char *columnsTableRecordData = columnsTableRecordItr + sizeof(char);

        // set nullfields to 0
        memset(columnsTableRecordItr, 0, sizeof(char));

        //copy data
        for (int index = 0; index < numFieldsInRecord; index++) {

            if (columnTableRecordDescriptor[index].name == "table-id") {
                memcpy(columnsTableRecordData, &tableIndex, sizeof(int));
                //std::cout <<"ID:"<< (*(int*)columnsTableRecordData)<<std::endl;
                columnsTableRecordData += sizeof(int);
            } else if (columnTableRecordDescriptor[index].name == "column-name") {
                std::string currentColumnName = currentRecordDescriptor[position - 1].name;
                //std::cout<<currentColumnName;
                int varCharLength = currentColumnName.length();
                //std::cout <<"lenght of col_name :"<< varCharLength<<" ";
                memcpy(columnsTableRecordData, &varCharLength, sizeof(int));
                columnsTableRecordData += sizeof(int);
                memcpy(columnsTableRecordData, currentRecordDescriptor[position - 1].name.c_str(), varCharLength);
                //std::cout <<"col_name :"<< (*(char*)columnsTableRecordData)<<std::endl;
                columnsTableRecordData += varCharLength;
            } else if (columnTableRecordDescriptor[index].name == "column-type") {
                //std::cout << "current descripter type :"<<currentRecordDescriptor[position].type<<" ";
                memcpy(columnsTableRecordData, &currentRecordDescriptor[position - 1].type, sizeof(int));
                //std::cout << *(&currentRecordDescriptor[position].type);
                columnsTableRecordData += sizeof(int);
            } else if (columnTableRecordDescriptor[index].name == "column-length") {
                //std::cout << currentRecordDescriptor[position].length<<std::endl;
                memcpy(columnsTableRecordData, &currentRecordDescriptor[position - 1].length, sizeof(int));
                //std::cout << *(&currentRecordDescriptor[position].type);
                columnsTableRecordData += sizeof(int);
            } else if (columnTableRecordDescriptor[index].name == "column-position") {
                memcpy(columnsTableRecordData, &position, sizeof(int));
                //std::cout << *(&position);
                columnsTableRecordData += sizeof(int);
            }
        }
    }

    RC RelationManager::insertRecordInTablesTable(const std::string &tableName, int &tableIndex) {
        FileHandle tablesTableFileHandle;
        // open the file
        if (rbfManagerPtr->openFile(tablesTable, tablesTableFileHandle) == -1) {
            // printf("unable to open tablestable ");
            return -1;
        }
        //create tablesTable record here
        void *tablesTableRecord = malloc(TABLES_RECORD_MAX_SIZE);
        createTablesTableRecord(tablesTableRecordDescriptor, tablesTableRecord, tableIndex, tableName);
        //insert tablesTable records here
        RID tableRid;
        // std::cout << "created tabelrecord ";
        if (rbfManagerPtr->insertRecord(tablesTableFileHandle, tablesTableRecordDescriptor, tablesTableRecord,
                                        tableRid) == -1) {
            free(tablesTableRecord);
            tablesTableRecord = NULL;
            //  printf("unable to insert in tablestable ");
            return -1;
        }
        //std::ostringstream out;
        //rbfManagerPtr->printRecord(tablesTableRecordDescriptor, tablesTableRecord, out);
        free(tablesTableRecord);
        tablesTableRecord = NULL;
        rbfManagerPtr->closeFile(tablesTableFileHandle);
        return 0;

    }

    RC RelationManager::insertRecordInColumnsTable(const std::vector<Attribute> &currentRecordDescriptor,
                                                   const std::string &tableName, int &tableIndex) {
        FileHandle columnsTableFileHandle;
        // open the file
        if (rbfManagerPtr->openFile(columnsTable, columnsTableFileHandle) == -1) {
            return -1;
        }
        //create columnsTable records for tablesTable
        int tablesNumFieldsInRecord = currentRecordDescriptor.size();

        for (int index = 0; index < tablesNumFieldsInRecord; index++) {
            void *columnsTableRecord = malloc(COLUMNS_RECORD_MAX_SIZE);
            int position = index + 1;
            createColumnsTableRecord(columnTableRecordDescriptor, currentRecordDescriptor, columnsTableRecord,
                                     tableIndex, tableName, position);
            RID tableRid;
            //insert columnsTable records for tablesTable
            if (rbfManagerPtr->insertRecord(columnsTableFileHandle, columnTableRecordDescriptor, columnsTableRecord,
                                            tableRid) == -1) {
                free(columnsTableRecord);
                columnsTableRecord = NULL;
                //printf("unable to insert in columns table ");
                return -1;
            }
            //std::ostringstream out;
            //rbfManagerPtr->printRecord(columnTableRecordDescriptor, columnsTableRecord, out);
            free(columnsTableRecord);
            columnsTableRecord = NULL;
        }
        rbfManagerPtr->closeFile(columnsTableFileHandle);
        return 0;
    }

    RC RelationManager::createCatalog() {
        //printf("enter create catalog");
        // TODO::system v/s user
        //create tablesTable
        if (rbfManagerPtr->createFile(tablesTable) > -1) {
            //create and insert tablesTable record here
            int tableIndex = 1;
            if (insertRecordInTablesTable(tablesTable, tableIndex) > -1) {
                //create and insert columns Table record here
                tableIndex = 2;
                if (insertRecordInTablesTable(columnsTable, tableIndex) > -1) {
                    //std::cout << "created columnrecord ";
                } else {
                    //printf("unable to insert in columns table ");
                    return -1;
                }
            } else {
                //printf("unable to insert in columns table ");
                return -1;
            }
        } else {
            //printf("unable to create tablestable ");
            return -1;
        }
        //create columnsTable
        if (rbfManagerPtr->createFile(columnsTable) > -1) {

            //create and insert tableTable records for columnsTable
            int tableIndex = 1;
            if (insertRecordInColumnsTable(tablesTableRecordDescriptor, tablesTable, tableIndex) > -1) {
                //create and insert columntable records for columnsTable
                tableIndex = 2;
                if (insertRecordInColumnsTable(columnTableRecordDescriptor, tablesTable, tableIndex) > -1) {
                    //columnsTableFileHandle.closeActivefile();
                    return 0;
                } else {
                    //printf("unable to insert in columns table ");
                    return -1;
                }
            } else {
                //printf("unable to insert in columns table ");
                return -1;
            }
        }
        //std::cout << "unable to create file";
        return -1;
    }

    RC RelationManager::deleteCatalog() {
        if (rbfManagerPtr->destroyFile(columnsTable) > -1) {
            if (rbfManagerPtr->destroyFile(tablesTable) > -1) {
                // printf("catalog destroyed");
                return 0;
            }
            // printf("unable to delete tablestable");
            return -1;
        }
        // printf("unable to delete columnstable");
        return -1;
    }

    RC RelationManager::getNextTableIndex(int &tableIndex) {
        //  printf("entered getnext table index");

        //scan through the file for largest tableID
        int largestTableID = 1;
        RID tablesTableRID;
        void *data = malloc(sizeof(char) + sizeof(int));
        RBFM_ScanIterator tablesTableScanIterator;
        std::vector<std::string> requiredAttributes;
        requiredAttributes.push_back(tablesTableRecordDescriptor[0].name);
        char emptyByte = 0;
        if (rbfManagerPtr->openFile(tablesTable, tablesTableScanIterator.fileHandle) == -1) {
            return -1;
        }
        rbfManagerPtr->scan(tablesTableScanIterator.fileHandle, tablesTableRecordDescriptor,
                            tablesTableRecordDescriptor[0].name, NO_OP, &emptyByte, requiredAttributes,
                            tablesTableScanIterator);
        while (tablesTableScanIterator.getNextRecord(tablesTableRID, data) != RM_EOF) {
            if ((*(char *) data) == 0) {
                int *tablesTableID = (int *) malloc(sizeof(int));
                memcpy(tablesTableID, (char *) data + sizeof(char), sizeof(int));
                if (largestTableID < *tablesTableID) largestTableID = *tablesTableID;
                free(tablesTableID);
                tablesTableID = NULL;
            }
        }
        tableIndex = largestTableID + 1;
        tablesTableScanIterator.close();
        free(data);
        data = NULL;
        return 0;

    }

    RC RelationManager::checkCatalogExists() {
        //check if catalog exists
        FileHandle tablesTableFileHandle;
        FileHandle columnsTableFileHandle;
        int tablesTableLen = this->tablesTable.length();
        int columnsTableLen = this->columnsTable.length();
        char tablesTablechar[tablesTableLen + 1];
        char columnsTablechar[columnsTableLen + 1];
        strcpy(tablesTablechar, this->tablesTable.c_str());
        strcpy(columnsTablechar, this->columnsTable.c_str());
        if (access(tablesTablechar, F_OK) != 0) {
            return -1;
        }
        if (access(columnsTablechar, F_OK) != 0) {
            return -1;
        }
        return 0;
    }

    RC RelationManager::createTable(const std::string &tableName, const std::vector<Attribute> &attrs) {

        if (checkCatalogExists() == -1) {
            return -1;
        }

        if (rbfManagerPtr->createFile(tableName) > -1) {
            //find next tableIndex
            // printf("create table entered");
            int tableIndex;
            getNextTableIndex(tableIndex);
            //insert new table in the catalog
            if (insertRecordInTablesTable(tableName, tableIndex) > -1) {
                if (insertRecordInColumnsTable(attrs, tableName, tableIndex) > -1) {
                    return 0;
                }
            }
            return -1;
        }
        return -1;
    }

    RC RelationManager::getRequiredTablesTableIdAndRid(const std::string &tableName, int &tableIndex,
                                                       RID &tablesTableRID) {

        //scan through the file for required ID
        void *data = malloc(sizeof(char) + sizeof(int));
        RBFM_ScanIterator tablesTableScanIterator;
        std::vector<std::string> requiredAttributes;
        requiredAttributes.push_back(tablesTableRecordDescriptor[0].name);
        //preparing value to recordCompare
        int varCharLength = tableName.length();
        char *tableNameToCompare = (char *) malloc(sizeof(int) + varCharLength);
        memcpy(tableNameToCompare, &varCharLength, sizeof(int));
        memcpy(tableNameToCompare + sizeof(int), tableName.c_str(), varCharLength);
        if (rbfManagerPtr->openFile(tablesTable, tablesTableScanIterator.fileHandle) == -1) {
            return -1;
        }
        //scanning required table name
        rbfManagerPtr->scan(tablesTableScanIterator.fileHandle, tablesTableRecordDescriptor,
                            tablesTableRecordDescriptor[1].name, EQ_OP, tableNameToCompare, requiredAttributes,
                            tablesTableScanIterator);
        while (tablesTableScanIterator.getNextRecord(tablesTableRID, data) != RM_EOF) {
            if (*((char *) data) == 0) {
                memcpy(&tableIndex, (char *) data + sizeof(char), sizeof(int));
            }
        }
        tablesTableScanIterator.close();
        free(data);
        free(tableNameToCompare);
        data = NULL;
        tableNameToCompare = NULL;
        return 0;
    }

    RC RelationManager::getRequiredColumnsTableRid(const std::string &tableName, const int &tableIndex,
                                                   std::vector<RID> &columnsTableRID) {


        void *data = malloc(sizeof(char) + sizeof(int));
        RBFM_ScanIterator columnsTableScanIterator;
        std::vector<std::string> requiredAttributes;
        requiredAttributes.push_back(columnTableRecordDescriptor[0].name);
        RID tempRID;
        // get required table index
        //scanning required table name
        if (rbfManagerPtr->openFile(columnsTable, columnsTableScanIterator.fileHandle) == -1) {
            return -1;
        }
        rbfManagerPtr->scan(columnsTableScanIterator.fileHandle, columnTableRecordDescriptor,
                            columnTableRecordDescriptor[0].name, EQ_OP, &tableIndex, requiredAttributes,
                            columnsTableScanIterator);
        while (columnsTableScanIterator.getNextRecord(tempRID, data) != RM_EOF) {
            columnsTableRID.push_back(tempRID);
        }
        columnsTableScanIterator.close();
        rbfManagerPtr->closeFile(columnsTableScanIterator.fileHandle);
        free(data);
        data = NULL;
        return 0;
    }

    RC RelationManager::deleteTable(const std::string &tableName) {
        // first delete the records in catalog
        //if tablename is catalog tablenames,return errors
        if (checkCatalogExists() == -1) {
            return -1;
        }
        if (tableName == tablesTable || tableName == columnsTable) {
            return -1;
        }
        RID tablesTableRID;
        int tableIndex;

        //destroy index
        std::vector<Attribute> tableRecordDescriptor;
        getAttributes(tableName, tableRecordDescriptor);
        int numFieldsInRecord = tableRecordDescriptor.size();
        int requiredAttributeIndex = -1;
        for (int Index = 0; Index < numFieldsInRecord; Index++) {
            bool isIndexPresent = false;
            std::string indexFileName = checkIndexExists(tableRecordDescriptor[Index].name, tableName, isIndexPresent);
            if (isIndexPresent) {
                destroyIndex(tableName, tableRecordDescriptor[Index].name);
            }
        }

        //scan the name through the tablesTable to get related ID
        getRequiredTablesTableIdAndRid(tableName, tableIndex, tablesTableRID);
        //delete the tuple in tablestable
        FileHandle tableFileHandle;
        FileHandle columnsFileHandle;
        if (rbfManagerPtr->openFile(tablesTable, tableFileHandle) == -1) {
            return -1;
        }
        if (rbfManagerPtr->deleteRecord(tableFileHandle, tablesTableRecordDescriptor, tablesTableRID) == -1) {
            rbfManagerPtr->closeFile(tableFileHandle);
            return -1;
        }

        std::vector<RID> columnsTableRID;
        //scan the TID in columnsTable
        getRequiredColumnsTableRid(tableName, tableIndex, columnsTableRID);
        //delete all the tuples in columnstable
        for (int index = 0; index < columnsTableRID.size(); index++) {

            if (rbfManagerPtr->openFile(columnsTable, columnsFileHandle) == -1) {
                return -1;
            }
            if (rbfManagerPtr->deleteRecord(columnsFileHandle, columnTableRecordDescriptor, columnsTableRID[index]) ==
                -1) {
                rbfManagerPtr->closeFile(columnsFileHandle);
                return -1;
            }
        }
        if (rbfManagerPtr->destroyFile(tableName) == -1) {
            return -1;
        }

        rbfManagerPtr->closeFile(columnsFileHandle);
        // printf("table deleted");
        return 0;
    }

    RC RelationManager::getAttributes(const std::string &tableName, std::vector<Attribute> &attrs) {
        //scan tablename in tablesTable to get ID
        if (checkCatalogExists() == -1) {
            return -1;
        }
        if (tableName == tablesTable) {
            attrs = tablesTableRecordDescriptor;
            return 0;
        }
        if (tableName == columnsTable) {
            attrs = columnTableRecordDescriptor;
            return 0;
        }
        //TODO :: Assuming that all the attributes will be present in columns table
        RID tablesTableRID;
        int tableIndex = 0;
        getRequiredTablesTableIdAndRid(tableName, tableIndex, tablesTableRID);
        std::vector<RID> columnsTableRID;
        getRequiredColumnsTableRid(tableName, tableIndex, columnsTableRID);

//        Attribute nullAttribute;
//        nullAttribute.name = "";
//        nullAttribute.type = TypeVarChar;
//        nullAttribute.length = 0;
        std::vector<Attribute> requiredAttributes(columnsTableRID.size());
        FileHandle columnsTableFileHandle;
        if (rbfManagerPtr->openFile(columnsTable, columnsTableFileHandle) == -1) {
            return -1;
        }

        //scan all the rows of ID in columns table and construct attribute
        for (int index = 0; index < columnsTableRID.size(); index++) {
            char *columnRecordData = (char *) malloc(COLUMNS_RECORD_MAX_SIZE);
            Attribute tempAttribute;
            int tempColumnPosition;
            int varCharLength;
            char *columnRecordDataItr = columnRecordData;
            int tempLength, tempType, tempPosition;

            if (rbfManagerPtr->readRecord(columnsTableFileHandle, columnTableRecordDescriptor, columnsTableRID[index],
                                          columnRecordData) == -1) {
                free(columnRecordData);
                columnRecordData = NULL;
                return -1;
            }

            // format - no.of feilds,nullbit - 1byte,pointers, table-id, column-name,column-type,column-length,column-position

            columnRecordDataItr += (sizeof(char) + sizeof(int));

            memcpy(&varCharLength, columnRecordDataItr, sizeof(int));
            columnRecordDataItr += sizeof(int);
            char *tempName = (char *) malloc(varCharLength + 1);
            memcpy(tempName, columnRecordDataItr, varCharLength);
            tempName[varCharLength] = '\0';
            tempAttribute.name = std::string(tempName);
            columnRecordDataItr += varCharLength;

            memcpy(&tempType, columnRecordDataItr, sizeof(int));
            tempAttribute.type = (AttrType) tempType;
            columnRecordDataItr += sizeof(int);

            memcpy(&tempLength, columnRecordDataItr, sizeof(int));
            tempAttribute.length = tempLength;
            columnRecordDataItr += sizeof(int);

            memcpy(&tempColumnPosition, columnRecordDataItr, sizeof(int));

            requiredAttributes[tempColumnPosition - 1] = tempAttribute;
            free(columnRecordData);
            free(tempName);
            columnRecordData = NULL;
            tempName = NULL;

        }
        rbfManagerPtr->closeFile(columnsTableFileHandle);
        attrs = requiredAttributes;
        return 0;
    }

    bool RelationManager::checkNullIndicator(const char *byte) {
        return (*byte) & (1 << 7);
    }

    RC RelationManager::insertTuple(const std::string &tableName, const void *data, RID &rid) {
        if (checkCatalogExists() == -1) {
            return -1;
        }
        if (tableName == tablesTable || tableName == columnsTable) {
            return -1;
        }
        FileHandle tableFileHandle;
        std::vector<Attribute> tableRecordDescriptor;
        getAttributes(tableName, tableRecordDescriptor);
        if (rbfManagerPtr->openFile(tableName, tableFileHandle) == FAILED) {
            return FAILED;
        }
        if (rbfManagerPtr->insertRecord(tableFileHandle, tableRecordDescriptor, data, rid) == -1) {
            rbfManagerPtr->closeFile(tableFileHandle);
            return -1;
        }
        rbfManagerPtr->closeFile(tableFileHandle);
        // insert tuple in index files
        int numFieldsInRecord = tableRecordDescriptor.size();
        int requiredAttributeIndex = -1;
        for (int Index = 0; Index < numFieldsInRecord; Index++) {
            bool isIndexPresent = false;
            std::string indexFileName = checkIndexExists(tableRecordDescriptor[Index].name, tableName, isIndexPresent);
            if (isIndexPresent) {
                // we got legit index , create tree
                IXFileHandle ixFileHandle;
                if (indexMangerPtr->openFile(indexFileName, ixFileHandle) == FAILED) {
                    return FAILED;
                }

                //insert key in index
                void *key = malloc(PAGE_SIZE);
                int RC = readAttribute(tableName, rid, tableRecordDescriptor[Index].name, key);
                if(RC==FAILED){
                    return FAILED;
                }
                char *keyToIndex = (char *) key + sizeof(char);

                //null indicator non-zero for NULL value
                char nullIndicator = *((char *) key);
                if (!checkNullIndicator(&nullIndicator)) {
                    indexMangerPtr->insertEntry(ixFileHandle, tableRecordDescriptor[Index], keyToIndex, rid);
                }
                indexMangerPtr->closeFile(ixFileHandle);
                loadedFree(&key);
            }
        }
        return PASS;

    }

    RC RelationManager::deleteTuple(const std::string &tableName, const RID &rid) {
        if (checkCatalogExists() == -1) {
            return -1;
        }
        if (tableName == tablesTable || tableName == columnsTable) {
            return -1;
        }
        FileHandle tableFileHandle;
        std::vector<Attribute> tableRecordDescriptor;
        getAttributes(tableName, tableRecordDescriptor);
        if (rbfManagerPtr->openFile(tableName, tableFileHandle) == -1) {
            return -1;
        }
        if (rbfManagerPtr->deleteRecord(tableFileHandle, tableRecordDescriptor, rid) == -1) {
            rbfManagerPtr->closeFile(tableFileHandle);
            return -1;
        }
        rbfManagerPtr->closeFile(tableFileHandle);
        // delete tuple in index files
        int numFieldsInRecord = tableRecordDescriptor.size();
        int requiredAttributeIndex = -1;
        for (int Index = 0; Index < numFieldsInRecord; Index++) {
            bool isIndexPresent = false;
            std::string indexFileName = checkIndexExists(tableRecordDescriptor[Index].name, tableName, isIndexPresent);
            if (isIndexPresent) {
                // we got legit index , create tree
                IXFileHandle ixFileHandle;
                if (indexMangerPtr->openFile(indexFileName, ixFileHandle) == FAILED) {
                    return FAILED;
                }

                //delete key in index
                void *key = malloc(PAGE_SIZE);
                int RC = readAttribute(tableName, rid, tableRecordDescriptor[Index].name, key);
                if(RC == FAILED){
                    return FAILED;
                }
                char *keyToIndex = (char *) key + sizeof(char);

                //null indicator non-zero for NULL value
                char nullIndicator = *((char *) key);
                if (!checkNullIndicator(&nullIndicator)) {
                    indexMangerPtr->deleteEntry(ixFileHandle, tableRecordDescriptor[Index], keyToIndex, rid);
                }
                indexMangerPtr->closeFile(ixFileHandle);
                loadedFree(&key);
            }
        }


        return PASS;
    }

    RC RelationManager::updateTuple(const std::string &tableName, const void *data, const RID &rid) {
        if (checkCatalogExists() == -1) {
            return -1;
        }
        if (tableName == tablesTable || tableName == columnsTable) {
            return -1;
        }
        FileHandle tableFileHandle;
        std::vector<Attribute> tableRecordDescriptor;
        getAttributes(tableName, tableRecordDescriptor);
        if (rbfManagerPtr->openFile(tableName, tableFileHandle) == -1) {
            return -1;
        }

        //delete index entry
        int numFieldsInRecord = tableRecordDescriptor.size();
        int requiredAttributeIndex = -1;
        for (int Index = 0; Index < numFieldsInRecord; Index++) {
            bool isIndexPresent = false;
            std::string indexFileName = checkIndexExists(tableRecordDescriptor[Index].name, tableName, isIndexPresent);
            if (isIndexPresent) {
                // we got legit index , create tree
                IXFileHandle ixFileHandle;
                if (indexMangerPtr->openFile(indexFileName, ixFileHandle) == FAILED) {
                    return FAILED;
                }

                //update key in index
                void *key = malloc(PAGE_SIZE);
                readAttribute(tableName, rid, tableRecordDescriptor[Index].name, key);
                char *keyToIndex = (char *) key + sizeof(char);

                //null indicator non-zero for NULL value
                char nullIndicator = *((char *) key);
                if (!checkNullIndicator(&nullIndicator)) {
                    indexMangerPtr->deleteEntry(ixFileHandle, tableRecordDescriptor[Index], keyToIndex, rid);
                }
                indexMangerPtr->closeFile(ixFileHandle);
                loadedFree(&key);
            }
        }

        //update the record
        if (rbfManagerPtr->updateRecord(tableFileHandle, tableRecordDescriptor, data, rid) == -1) {
            rbfManagerPtr->closeFile(tableFileHandle);
            return -1;
        }

        // insert tuple in index files
        numFieldsInRecord = tableRecordDescriptor.size();
        requiredAttributeIndex = -1;
        for (int Index = 0; Index < numFieldsInRecord; Index++) {
            bool isIndexPresent = false;
            std::string indexFileName = checkIndexExists(tableRecordDescriptor[Index].name, tableName, isIndexPresent);
            if (isIndexPresent) {
                // we got legit index , create tree
                IXFileHandle ixFileHandle;
                if (indexMangerPtr->openFile(indexFileName, ixFileHandle) == FAILED) {
                    return FAILED;
                }

                //update key in index
                void *key = malloc(PAGE_SIZE);
                readAttribute(tableName, rid, tableRecordDescriptor[Index].name, key);
                char *keyToIndex = (char *) key + sizeof(char);

                //null indicator non-zero for NULL value
                char nullIndicator = *((char *) key);
                if (!checkNullIndicator(&nullIndicator)) {
                    indexMangerPtr->insertEntry(ixFileHandle, tableRecordDescriptor[Index], keyToIndex, rid);
                }
                indexMangerPtr->closeFile(ixFileHandle);
                loadedFree(&key);
            }
        }

        rbfManagerPtr->closeFile(tableFileHandle);
        return 0;
    }

    RC RelationManager::readTuple(const std::string &tableName, const RID &rid, void *data) {
        FileHandle tableFileHandle;
        std::vector<Attribute> tableRecordDescriptor;
        getAttributes(tableName, tableRecordDescriptor);
        if (rbfManagerPtr->openFile(tableName, tableFileHandle) == -1) {
            return -1;
        }
        if (rbfManagerPtr->readRecord(tableFileHandle, tableRecordDescriptor, rid, data) == -1) {
            rbfManagerPtr->closeFile(tableFileHandle);
            return -1;
        }
        rbfManagerPtr->closeFile(tableFileHandle);
        return -0;
    }

    RC RelationManager::printTuple(const std::vector<Attribute> &attrs, const void *data, std::ostream &out) {
        if (rbfManagerPtr->printRecord(attrs, data, out) > -1) {
            return 0;
        }
        return -1;
    }

    RC RelationManager::readAttribute(const std::string &tableName, const RID &rid, const std::string &attributeName,
                                      void *data) {
        FileHandle tableFileHandle;
        std::vector<Attribute> tableRecordDescriptor;
        getAttributes(tableName, tableRecordDescriptor);
        if (rbfManagerPtr->openFile(tableName, tableFileHandle) == -1) {
            return -1;
        }
        if (rbfManagerPtr->readAttribute(tableFileHandle, tableRecordDescriptor, rid, attributeName, data) == -1) {
            rbfManagerPtr->closeFile(tableFileHandle);
            return -1;
        }
        rbfManagerPtr->closeFile(tableFileHandle);
        return 0;
    }

    RC RelationManager::scan(const std::string &tableName, const std::string &conditionAttribute, const CompOp compOp,
                             const void *value, const std::vector<std::string> &attributeNames,
                             RM_ScanIterator &rm_ScanIterator) {

//        FileHandle tableFileHandle;
        if (rbfManagerPtr->openFile(tableName, rm_ScanIterator.tableFileHandle) != PASS) {
            return FAILED;
        }

        std::vector<Attribute> tableDescripter;
        getAttributes(tableName, tableDescripter);
        rbfManagerPtr->scan(rm_ScanIterator.tableFileHandle, tableDescripter, conditionAttribute, compOp, value,
                            attributeNames, rm_ScanIterator.rbfManagerScanIterator);
//            rbfManagerPtr->closeFile(tableFileHandle);
//            return -1;

//        rbfManagerPtr->closeFile(tableFileHandle);
        return PASS;
    }

    RM_ScanIterator::RM_ScanIterator() = default;

    RM_ScanIterator::~RM_ScanIterator() = default;

    RC RM_ScanIterator::getNextTuple(RID &rid, void *data) {
        return this->rbfManagerScanIterator.getNextRecord(rid, data);
    }

    RC RM_ScanIterator::close() {
        // TODO::fileHandle
        if (rbfManagerScanIterator.close() == -1) {
            return -1;
        }
        return 0;
    }

    std::string
    RelationManager::checkIndexExists(const std::string &indexAttribute, const std::string &tableNameToCheck,
                                      bool &isIndexPresent) {
        std::string indexName = "index_" + tableNameToCheck + "_" + indexAttribute + ".idx";
        struct stat buff;
        if (stat(indexName.c_str(), &buff) == 0) {
            isIndexPresent = true;
        } else {
            isIndexPresent = false;
        }
        return indexName;
    }

    RC RelationManager::createIndex(const std::string &tableName, const std::string &attributeName) {
        bool isIndexPresent = false;
        Attribute requiredAttribute;
        // check if index file already exists
        std::string indexFileName = checkIndexExists(attributeName, tableName, isIndexPresent);

        if (isIndexPresent) {
            return FAILED;
        }
        if (indexMangerPtr->createFile(indexFileName) == FAILED) {
            return FAILED;
        }

        if (!getAttributeFromAttName(tableName, requiredAttribute, attributeName)) {
            return FAILED;
        }

        // we got legit index , create tree
        IXFileHandle ixFileHandle;
        if (indexMangerPtr->openFile(indexFileName, ixFileHandle) == FAILED) {
            return FAILED;
        }

        //Scan
        std::vector<std::string> reqAttrVector;
        reqAttrVector.push_back(requiredAttribute.name);
        RM_ScanIterator rm_ScanIterator;
        if (scan(tableName, "", NO_OP, NULL, reqAttrVector, rm_ScanIterator) == FAILED) {
            return FAILED;
        }
        RID rid;
        void *reqAttInfo = malloc(PAGE_SIZE);
        while (rm_ScanIterator.getNextTuple(rid, reqAttInfo) != RM_EOF) {
            char *key = (char *) reqAttInfo + sizeof(char);

            //null indicater non-zero for NULL value
            char nullIndicator = *((char *) reqAttInfo);
            if (!checkNullIndicator(&nullIndicator)) {
                indexMangerPtr->insertEntry(ixFileHandle, requiredAttribute, key, rid);
                memset(reqAttInfo, 0, PAGE_SIZE);
            }
        }
        if (indexMangerPtr->closeFile(ixFileHandle) == FAILED) {
            return FAILED;
        }
        loadedFree(&reqAttInfo);
        if (rm_ScanIterator.close() == FAILED) {
            return FAILED;
        }
        return PASS;

    }

    RC RelationManager::destroyIndex(const std::string &tableName, const std::string &attributeName) {
        bool isIndexPresent = false;
        // check if index file exists
        std::string indexFileName = checkIndexExists(attributeName, tableName, isIndexPresent);

        if (!isIndexPresent) {
            return FAILED;
        }

        if (indexMangerPtr->destroyFile(indexFileName) == FAILED) {
            return FAILED;
        }
        return PASS;

    }

    bool RelationManager::getAttributeFromAttName(const std::string &tableName, Attribute &requiredAttribute,
                                                  const std::string &attributeName) {
        int requiredAttributeIndex = -1;
        int numFieldsInRecord = 0;
        bool attFound = false;
        //Get index of attribute to be indexed from record descriptor
        std::vector<Attribute> tableRecordDescriptor;
        getAttributes(tableName, tableRecordDescriptor);
        numFieldsInRecord = tableRecordDescriptor.size();
        for (int requiredIndex = 0; requiredIndex < numFieldsInRecord; requiredIndex++) {
            if (tableRecordDescriptor[requiredIndex].name.compare(attributeName) == 0) {
                requiredAttributeIndex = requiredIndex;
                requiredAttribute.name = tableRecordDescriptor[requiredIndex].name;
                requiredAttribute.type = tableRecordDescriptor[requiredIndex].type;
                requiredAttribute.length = tableRecordDescriptor[requiredIndex].length;
                attFound = true;
                break;
            }
        }
        return attFound;
    }

    RC RelationManager::indexScan(const std::string &tableName, const std::string &attributeName, const void *lowKey,
                                  const void *highKey, bool lowKeyInclusive, bool highKeyInclusive,
                                  RM_IndexScanIterator &rm_IndexScanIterator) {
        bool isIndexPresent = false;
        Attribute requiredAttribute;
        std::string indexFileName = checkIndexExists(attributeName, tableName, isIndexPresent);
        if (!isIndexPresent) { return FAILED; }


        if (indexMangerPtr->openFile(indexFileName, rm_IndexScanIterator.ixFileHandle) == FAILED) {
            return FAILED;
        }
        if (!getAttributeFromAttName(tableName, requiredAttribute, attributeName)) {
            return FAILED;
        }
        indexMangerPtr->scan(rm_IndexScanIterator.ixFileHandle, requiredAttribute, lowKey, highKey, lowKeyInclusive, highKeyInclusive,
                             rm_IndexScanIterator.scanIterator);
        return PASS;
    }

    // Extra credit work
    RC RelationManager::dropAttribute(const std::string &tableName, const std::string &attributeName) {
        return -1;
    }

    // Extra credit work
    RC RelationManager::addAttribute(const std::string &tableName, const Attribute &attr) {
        return -1;
    }

    RC RM_IndexScanIterator::getNextEntry(RID &rid, void *key) {
        return this->scanIterator.getNextEntry(rid, key);
    }

    RC RM_IndexScanIterator::close() {
         this->scanIterator.close();
         indexMangerPtr->closeFile(this->ixFileHandle);
    }

} // namespace PeterDB