#include "src/include/qe.h"

namespace PeterDB {
    void Filter::setupFilter(const Condition &condition) {
        this->condition = condition;
        this->input->getAttributes(this->filterRecordDescriptor);
    }

    Filter::Filter(Iterator *input, const Condition &condition) {
        this->input = input;
        filterRecordDescriptor.clear();
        setupFilter(condition);
    }

    Filter::~Filter() {

    }

//    bool Filter::isConditionMet(void *lhs, void *rhs, AttrType &attributeType) {
//
//    }

    bool handleEmptyExpectedRecordComparision(void *Value, Condition &condition) {
        //No value in expected record case
        if (condition.op == NO_OP) {
            return true;
        }
        std::set<int> eqSet = {EQ_OP, LE_OP, GE_OP, NE_OP};
        bool IsPresentInSet = (eqSet.find(condition.op) != eqSet.end());
        if (IsPresentInSet)
            return Value == NULL;
        return false;

    }

    bool handleNullMembers(char *lhsRecord, char *rhsRecord, Condition &condition) {
        if (rhsRecord == NULL) {
            return handleEmptyExpectedRecordComparision(lhsRecord, condition);
        } else if (lhsRecord == NULL) {
            return handleEmptyExpectedRecordComparision(rhsRecord, condition);
        }
        return false;
    }

    RC floatCompare(float actual, float expected) {
        if (fabs(actual - expected) < DELTA) {
            return 0;
        } else if (actual < expected) {
            return -1;
        } else {
            return 1;
        }
    }

    bool recordCompare(const void *lhsRecord, const void *rhsRecord, const AttrType attType, Condition &condition) {
        if (condition.op == NO_OP) {
            return true;
        }
        char *expectedRecordDataPtr = (char *) rhsRecord;

        //Handle empty cases
        if (rhsRecord == NULL || lhsRecord == NULL) {
            return handleNullMembers((char *) lhsRecord, (char *) rhsRecord, condition);
        }

        int actualCompValue = 0;
        if (attType == TypeInt) {
            actualCompValue = *((int *) lhsRecord) - *((int *) expectedRecordDataPtr);

        } else if (attType == TypeReal) {
            actualCompValue = floatCompare(*((float *) lhsRecord), *((float *) expectedRecordDataPtr));
        } else if (attType == TypeVarChar) {
            //original string (expects null terminated string)
            int lengthOriginalVarchar = *((int *) lhsRecord);
            char *originalVarcharPtr = (char *) lhsRecord + sizeof(int);
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
        switch (condition.op) {
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

    bool getAttributeFromAttName(Attribute &requiredAttribute, const std::string &attributeName,
                                 const std::vector <Attribute> tableRecordDescriptor) {
        int requiredAttributeIndex = -1;
        int numFieldsInRecord = 0;
        bool attFound = false;
        //Get index of attribute to be indexed from record descriptor
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

    bool checkBitSet(int index, char *inputRecordBitMap) {
        int reqByteNum = index / 8;
        int reminder = index % 8;
        char reqByte = *(inputRecordBitMap + reqByteNum);
        return (reqByte >> (7 - reminder)) & 1;
    }

    RC getAttributeValueFromData(const std::vector <Attribute> recordDescriptor, const std::string &attributeName,
                                 void *inputData, void *lhsValue) {
        //get index of the required attribute
        int requiredIndex = -1;
        char *valuePtr = (char *) inputData;
        char *lhsValuePtr = (char *) lhsValue;
        char *nullIndicatorPtr = (char *) inputData;
        //sizes
        int numFieldsInRecord = recordDescriptor.size();
        int nullIndicatorSize = ceil((float) numFieldsInRecord / 8);
        valuePtr += nullIndicatorSize;
        for (int index = 0; index < recordDescriptor.size(); index++) {
            if (checkBitSet(index, nullIndicatorPtr)) {
                continue;
            }
            if (recordDescriptor[index].name == attributeName) {
                requiredIndex = index;
                break;
            }
            if (recordDescriptor[index].type == TypeInt) {
                valuePtr += sizeof(int);
            } else if (recordDescriptor[index].type == TypeVarChar) {
                int varCharLength = *((int *) valuePtr);
                valuePtr += sizeof(int) + varCharLength;
            } else if (recordDescriptor[index].type == TypeReal) {
                valuePtr += sizeof(float);
            }
        }
        if (requiredIndex == -1) {
            return FAILED;
        }

        if (!checkBitSet(requiredIndex, nullIndicatorPtr)) {
            //first filed is NULL indicator
            char nullBit = 0;
            memcpy(lhsValuePtr, &nullBit, sizeof(char));
            lhsValuePtr += sizeof(char);
            if (recordDescriptor[requiredIndex].type == TypeInt) {
                memcpy(lhsValuePtr, valuePtr, sizeof(int));
            } else if (recordDescriptor[requiredIndex].type == TypeVarChar) {
                int varCharLength = *((int *) valuePtr);
                memcpy(lhsValuePtr, &varCharLength, sizeof(int));
                valuePtr += sizeof(int);
                lhsValuePtr += sizeof(int);
                memcpy(lhsValuePtr, valuePtr, varCharLength);
            } else if (recordDescriptor[requiredIndex].type == TypeReal) {
                memcpy(lhsValuePtr, valuePtr, sizeof(float));
            }
        } else {
            //its a NULL
            char reBytePtr = 0;
            (reBytePtr) |= (1 << 7);
            memcpy(lhsValuePtr, &reBytePtr, sizeof(char));
        }
        return 0;

    }

    bool Filter::checkNullIndicator(const char *byte) {
        return (*byte) & (1 << 7);
    }

    RC Filter::getNextTuple(void *data) {
        bool conditionSatisfied = false;
        while (this->input->getNextTuple(data) != QE_EOF) {
            void *lhsValueMem = malloc(PAGE_SIZE);
            void *rhsValue = this->condition.rhsValue.data;
            char *lhsValue = NULL;
            Attribute requiredAttribute;
            getAttributeFromAttName(requiredAttribute, condition.lhsAttr, filterRecordDescriptor);
            // get lhs value
            getAttributeValueFromData(filterRecordDescriptor, condition.lhsAttr, data, lhsValueMem);
            // in lhs , first byte is null inidicater, so pass rest based on it
            //check bit and pass it
            if (!checkNullIndicator((char *) lhsValueMem)) {
                lhsValue = (char *) lhsValueMem + sizeof(char);
            }
            bool conditionSatisfied = recordCompare(lhsValue, rhsValue, requiredAttribute.type, condition);
            loadedFree(&lhsValueMem);
            if (conditionSatisfied)
                return PASS;
        }
        return QE_EOF;
    }

    RC Filter::getAttributes(std::vector <Attribute> &attrs) const {
        return this->input->getAttributes(attrs);
    }

    void Project::setupProject() {
        this->input->getAttributes(this->recordDescriptor);
        for (int indexAtt = 0; indexAtt < this->attrNames.size(); indexAtt++) {
            for (int indexRD = 0; indexRD < this->recordDescriptor.size(); indexRD++) {
                if (this->recordDescriptor[indexRD].name == this->attrNames[indexAtt]) {
                    this->projectRecordDescriptor.push_back(this->recordDescriptor[indexRD]);
                    this->filteredNeededAttributeList.push_back(indexRD);
                }
            }
        }
    }

    Project::Project(Iterator *input, const std::vector <std::string> &attrNames) {
        this->input = input;
        this->attrNames = attrNames;
        this->projectRecordDescriptor.clear();
        this->recordDescriptor.clear();
        this->filteredNeededAttributeList.clear();
        setupProject();
    }

    Project::~Project() {
    }

    RC Project::prepareProjectedRecord(void *inputData, void *projectedRecord) {
        // Calculating index of the bytes location
        char *recordStartPointer = (char *) inputData;
        int numFieldsInRecord = recordDescriptor.size();
        int nullIndicatorSize = ceil((float) numFieldsInRecord / 8);
        char *nullIndicatorPtr = recordStartPointer;

        //set bit map
        int counter = 0;
        char *dataItr = (char *) projectedRecord;

        int outputNullIndicatorSize = ceil((float) projectRecordDescriptor.size() / 8);
        memset(dataItr, 0, outputNullIndicatorSize);

        for (auto index: filteredNeededAttributeList) {
            int reqByteNum = counter / 8;
            int bitToSetInByte = 7 - (counter % 8);
            char *reBytePtr = dataItr + reqByteNum * (sizeof(char));
            if (checkBitSet(index, nullIndicatorPtr)) {
                (*reBytePtr) |= (1 << bitToSetInByte);
            } else {
                (*reBytePtr) &= ~(1 << bitToSetInByte);
            }
            counter++;
        }

        dataItr += outputNullIndicatorSize;

        for (auto att: this->projectRecordDescriptor) {
            char *recordItr = (char *) inputData + outputNullIndicatorSize;
            for (int recordIndex = 0; recordIndex < this->recordDescriptor.size(); recordIndex++) {

                bool isNUll = false;
                if (checkBitSet(recordIndex, nullIndicatorPtr)) {
                    isNUll = true;
                }
                bool copyFlag = false;
                if ((this->recordDescriptor[recordIndex].name == att.name) && !isNUll) {
                    copyFlag = true;
                }

                if (recordDescriptor[recordIndex].type == TypeInt) {
                    int inc = isNUll ? 0 : sizeof(int);
                    if (copyFlag) {
                        memcpy(dataItr, recordItr, sizeof(int));
                        dataItr += inc;
                    }
                    recordItr += inc;
                } else if (recordDescriptor[recordIndex].type == TypeVarChar) {
                    int inc = isNUll ? 0 : sizeof(int);
                    int length = *((int *) recordItr);
                    if (copyFlag) {
                        memcpy(dataItr, &length, sizeof(int));
                        dataItr += inc;
                    }
                    recordItr += inc;
                    inc = isNUll ? 0 : length;
                    if (copyFlag) {
                        memcpy(dataItr, recordItr, length);
                        dataItr += inc;
                    }
                    recordItr += inc;
                } else if (recordDescriptor[recordIndex].type == TypeReal) {
                    int inc = isNUll ? 0 : sizeof(float);
                    if (copyFlag) {
                        memcpy(dataItr, recordItr, sizeof(float));
                        dataItr += inc;
                    }
                    recordItr += inc;
                }
            }
        }
        return PASS;

    }

    RC Project::getNextTuple(void *data) {
        void *inputRecord = malloc(PAGE_SIZE);
        void *projectRecord = data;
        if (this->input->getNextTuple(inputRecord) == QE_EOF) {
            return QE_EOF;
        }
        prepareProjectedRecord(inputRecord, projectRecord);
        loadedFree(&inputRecord);
        return PASS;
    }

    RC Project::getAttributes(std::vector <Attribute> &attrs) const {
        attrs = projectRecordDescriptor;
        return PASS;
    }

    void
    mergeRecordDescriptors(std::vector <Attribute> &rd1, std::vector <Attribute> &rd2, std::vector <Attribute> &out) {
        out.clear();
        out.insert(out.end(), rd1.begin(), rd1.end());
        out.insert(out.end(), rd2.begin(), rd2.end());
    }

    void BNLJoin::setUp() {
        this->leftIn->getAttributes(this->leftTableRecordDescriptor);
        this->rightIn->getAttributes(this->rightTableRecordDescriptor);
        mergeRecordDescriptors(this->leftTableRecordDescriptor, this->rightTableRecordDescriptor,
                               this->joinedTableRecordDescriptor);
        for (auto attr1: this->leftTableRecordDescriptor) {
            if (attr1.name.compare(this->condition.lhsAttr) == 0) {
                this->leftConditionAttribute = attr1;
            }
        }
        for (auto attr: this->rightTableRecordDescriptor) {
            if (attr.name.compare(this->condition.rhsAttr) == 0) {
                this->rightConditionAttribute = attr;
            }
        }
    }

    RC BNLJoin::clearHashMap() {
        for (auto &keyValuePairIt: this->intLeftBlockHashMap) {
            for (auto recordPtr: keyValuePairIt.second) {
                loadedFree(&recordPtr);
            }
        }
        this->intLeftBlockHashMap.clear();


        for (auto &keyValuePairIt: this->floatLeftBlockHashMap) {
            for (auto recordPtr: keyValuePairIt.second) {
                loadedFree(&recordPtr);
            }
        }
        this->floatLeftBlockHashMap.clear();

        for (auto &keyValuePairIt: this->stringLeftBlockHashMap) {
            for (auto recordPtr: keyValuePairIt.second) {
                loadedFree(&recordPtr);
            }
        }
        this->stringLeftBlockHashMap.clear();

        return PASS;
    }

    int BNLJoin::getRecordSize(void *record) {
        // Calculating index of the bytes location
        char *recordStart = (char *) record;
        int numFieldsInRecord = this->leftTableRecordDescriptor.size();
        int nullIndicatorSize = ceil((float) numFieldsInRecord / 8);

        int recordSize = nullIndicatorSize;
        int itr = nullIndicatorSize;
        for (int index = 0; index < numFieldsInRecord; index++) {
            if (!checkBitSet(index, (char *) record)) {

                if (this->leftTableRecordDescriptor[index].type == TypeInt) {
                    recordSize += sizeof(int);
                    itr += sizeof(int);
                } else if (this->leftTableRecordDescriptor[index].type == TypeReal) {
                    recordSize += sizeof(float);
                    itr += sizeof(float);
                } else if (this->leftTableRecordDescriptor[index].type == TypeVarChar) {
                    int length = *((int *) (recordStart + itr));
                    itr += sizeof(int);
                    itr += length;
                    recordSize += length;
                }
            }
        }
        return recordSize;
    }

    std::string charArrayToString(void *recordStart) {
        std::string resultStr;
        int length = *((int *) recordStart);
        char result[length + 1];
        result[length] = '\0';
        memcpy(result, (char *) recordStart + sizeof(int), length);
        resultStr = result;
        return resultStr;
    }

    RC BNLJoin::prepareLeftBlock(const unsigned numPages) {
        if (this->stringLeftBlockHashMap.empty() && this->floatLeftBlockHashMap.empty() &&
            this->intLeftBlockHashMap.empty() && !dataLeftInLeftTable) {
            dataPresentInHashMapBlock = false;
        }

        int presenttotalSize = 0;
        void *leftRecordTempBuffer = malloc(PAGE_SIZE);
        void *leftKeyTempBuffer = malloc(PAGE_SIZE);
        do {
            int RC = this->leftIn->getNextTuple(leftRecordTempBuffer);
            if (RC == QE_EOF) {
                dataLeftInLeftTable = false;
                break;
            }
            int presentSize = getRecordSize(leftRecordTempBuffer);
            presenttotalSize += presentSize;
            GetAttributeValueFromData(this->leftTableRecordDescriptor, this->leftConditionAttribute.name,
                                      leftRecordTempBuffer, leftKeyTempBuffer);
            void *actualLeftRecord = malloc(presentSize);
            memcpy(actualLeftRecord, leftRecordTempBuffer, presentSize);
            switch (this->leftConditionAttribute.type) {
                case TypeInt: {
                    int intKey = *((int *) leftKeyTempBuffer);
                    if (this->intLeftBlockHashMap.find(intKey) == this->intLeftBlockHashMap.end()) {
                        this->intLeftBlockHashMap[intKey] = std::vector<void *>();
                    }
                    this->intLeftBlockHashMap[intKey].push_back(actualLeftRecord);
                    break;
                }
                case TypeReal: {
                    float floatKey = *((float *) leftKeyTempBuffer);
                    if (this->floatLeftBlockHashMap.find(floatKey) == this->floatLeftBlockHashMap.end()) {
                        this->floatLeftBlockHashMap[floatKey] = std::vector<void *>();
                    }
                    this->floatLeftBlockHashMap[floatKey].push_back(actualLeftRecord);
                    break;
                }
                case TypeVarChar: {
                    std::string stringKey = charArrayToString(leftKeyTempBuffer);
                    if (this->stringLeftBlockHashMap.find(stringKey) == this->stringLeftBlockHashMap.end()) {
                        this->stringLeftBlockHashMap[stringKey] = std::vector<void *>();
                    }
                    this->stringLeftBlockHashMap[stringKey].push_back(actualLeftRecord);
                    break;
                }
            }
        } while (presenttotalSize <= (this->numPages * PAGE_SIZE) && dataLeftInLeftTable);
//        if (this->stringLeftBlockHashMap.empty() && this->floatLeftBlockHashMap.empty() &&
//            this->intLeftBlockHashMap.empty() && !dataLeftInLeftTable) {
//            dataPresentInHashMapBlock = false;
//        }
        loadedFree(&leftRecordTempBuffer);
        loadedFree(&leftKeyTempBuffer);
        return PASS;
    }

    BNLJoin::BNLJoin(Iterator *leftIn, TableScan *rightIn, const Condition &condition, const unsigned int numPages) {
        this->leftIn = leftIn;
        this->rightIn = rightIn;
        this->condition = condition;
        this->numPages = numPages;
        this->rightRecord = malloc(PAGE_SIZE);
        this->leftRecord = malloc(PAGE_SIZE);
        this->rightAttributeValue = malloc(PAGE_SIZE);
        this->presentIndexWithGivenKey = 0;
        this->dataLeftInLeftTable = true;
        this->currentVectorForRightExhaused = true;
        this->dataLeftInRightTable = false;
        this->dataPresentInHashMapBlock = true;
        setUp();
    }

    BNLJoin::~BNLJoin() {
        loadedFree(&this->rightRecord);
        loadedFree(&this->leftRecord);
        loadedFree(&this->rightAttributeValue);
    }

    bool BNLJoin::searchForRightRecordInHashMap(void **leftRecordPtr) {
        switch (this->leftConditionAttribute.type) {
            case TypeInt: {
                int intKey = *((int *) this->rightAttributeValue);
                if (this->intLeftBlockHashMap.find(intKey) == this->intLeftBlockHashMap.end()) {
                    return false;
                } else {
//                    std::cout<<"size is"<<intLeftBlockHashMap[intKey].size()<<std::endl;
//                    std::cout<<"key :: "<<intKey<<"  size :: "<<intLeftBlockHashMap[intKey].size()<< " pointer :: "<<presentIndexWithGivenKey<<std::endl;
                    if (presentIndexWithGivenKey < (this->intLeftBlockHashMap[intKey].size())) {
                        this->currentVectorForRightExhaused = false;
                        *leftRecordPtr = this->intLeftBlockHashMap[intKey][presentIndexWithGivenKey];
                        presentIndexWithGivenKey++;
                        return true;
                    } else {
                        this->currentVectorForRightExhaused = true;
                        presentIndexWithGivenKey = 0;
                        return false;
                    }
                }
                break;
            }
            case TypeReal: {
                float floatKey = *((float *) this->rightAttributeValue);
                if (this->floatLeftBlockHashMap.find(floatKey) == this->floatLeftBlockHashMap.end()) {
                    return false;
                } else {
                    if (presentIndexWithGivenKey == this->floatLeftBlockHashMap[floatKey].size()) {
                        presentIndexWithGivenKey = 0;
                        return false;
                    }

                    *leftRecordPtr = this->floatLeftBlockHashMap[floatKey][presentIndexWithGivenKey];
                    presentIndexWithGivenKey++;
                    return true;
                }
                break;
            }
            case TypeVarChar: {
                std::string stringKey = charArrayToString(this->rightAttributeValue);
                if (this->stringLeftBlockHashMap.find(stringKey) == this->stringLeftBlockHashMap.end()) {
                    return false;
                } else {
                    if (presentIndexWithGivenKey == this->stringLeftBlockHashMap[stringKey].size()) {
                        presentIndexWithGivenKey = 0;
                        return false;
                    }

                    *leftRecordPtr = this->stringLeftBlockHashMap[stringKey][presentIndexWithGivenKey];
                    presentIndexWithGivenKey++;
                    return true;
                }
                break;
            }
        }
        return PASS;
    }

    RC BNLJoin::getNextTuple(void *data) {
        if (!this->dataPresentInHashMapBlock) {
            return QE_EOF;
        }
        while (this->dataPresentInHashMapBlock) {
            if (!this->dataLeftInRightTable) {
                clearHashMap();
                int RC = prepareLeftBlock(numPages);
                if (!this->dataPresentInHashMapBlock) {
                    return QE_EOF;
                }
                this->rightIn->setIterator();
                this->dataLeftInRightTable = true;
            }
            int rightRC = 0;
            this->currentVectorForRightExhaused = true;
            do {
                if (currentVectorForRightExhaused) {
                    rightRC = this->rightIn->getNextTuple(rightRecord);
                    presentIndexWithGivenKey = 0;
                }
                void *leftRecordPtr = NULL;
                GetAttributeValueFromData(rightTableRecordDescriptor, rightConditionAttribute.name, this->rightRecord,
                                          this->rightAttributeValue);
                bool correspondingLeftRecordFound = searchForRightRecordInHashMap(&leftRecordPtr);
                if (correspondingLeftRecordFound) {
                    mergeRecords(leftTableRecordDescriptor, rightTableRecordDescriptor, leftRecordPtr, rightRecord,
                                 data);
                    return PASS;
                } else {
                    continue;
                }
            } while (rightRC != QE_EOF);

            this->dataLeftInRightTable = false;
        }
        return PASS;
    }

    RC BNLJoin::getAttributes(std::vector <Attribute> &attrs) const {
        attrs = this->joinedTableRecordDescriptor;
        return PASS;
    }

    RC mergeRecords(std::vector <Attribute> &leftRecordDescriptor, std::vector <Attribute> &rightRecordDescriptor,
                    void *leftRecord, void *rightRecord, void *resultRecord) {
        // Calculating left record detials
        char *leftrecordStartPointer = (char *) leftRecord;
        int leftnumFieldsInRecord = leftRecordDescriptor.size();
        int leftnullIndicatorSize = ceil((float) leftnumFieldsInRecord / 8);
        char *leftnullIndicatorPtr = leftrecordStartPointer;

        // Calculating right record detials
        char *rightrecordStartPointer = (char *) rightRecord;
        int rightnumFieldsInRecord = rightRecordDescriptor.size();
        int rightnullIndicatorSize = ceil((float) rightnumFieldsInRecord / 8);
        char *rightnullIndicatorPtr = rightrecordStartPointer;

        // Calculating result record detials
        char *resultrecordStartPointer = (char *) resultRecord;
        int resultnumFieldsInRecord = leftnumFieldsInRecord + rightnumFieldsInRecord;
        int resultnullIndicatorSize = ceil((float) resultnumFieldsInRecord / 8);
        char *resultnullIndicatorPtr = resultrecordStartPointer;

        //set bit map
        int counter = 0;
        char *dataItr = (char *) resultRecord;
        memset(dataItr, 0, resultnullIndicatorSize);

        for (int i = 0; i < leftnumFieldsInRecord; i++) {
            int reqByteNum = counter / 8;
            int bitToSetInByte = 7 - (counter % 8);
            char *reBytePtr = dataItr + reqByteNum * (sizeof(char));
            if (checkBitSet(i, leftnullIndicatorPtr)) {
                (*reBytePtr) |= (1 << bitToSetInByte);
            } else {
                (*reBytePtr) &= ~(1 << bitToSetInByte);
            }
            counter++;
        }

        for (int i = 0; i < rightnumFieldsInRecord; i++) {
            int reqByteNum = counter / 8;
            int bitToSetInByte = 7 - (counter % 8);
            char *reBytePtr = dataItr + reqByteNum * (sizeof(char));
            if (checkBitSet(i, rightnullIndicatorPtr)) {
                (*reBytePtr) |= (1 << bitToSetInByte);
            } else {
                (*reBytePtr) &= ~(1 << bitToSetInByte);
            }
            counter++;
        }

        char *leftValueItr = leftrecordStartPointer + leftnullIndicatorSize;
        char *rightValueItr = rightrecordStartPointer + rightnullIndicatorSize;
        char *resultValueItr = dataItr + resultnullIndicatorSize;

        // now data from left record
        for (int i = 0; i < leftnumFieldsInRecord; i++) {
            if (!checkBitSet(i, leftnullIndicatorPtr)) {
                if (leftRecordDescriptor[i].type == TypeInt) {
                    memcpy(resultValueItr, leftValueItr, sizeof(int));
                    resultValueItr += sizeof(int);
                    leftValueItr += sizeof(int);
                } else if (leftRecordDescriptor[i].type == TypeVarChar) {
                    int varCharLength = *((int *) leftValueItr);
                    memcpy(resultValueItr, &varCharLength, sizeof(int));
                    leftValueItr += sizeof(int);
                    resultValueItr += sizeof(int);
                    memcpy(resultValueItr, leftValueItr, varCharLength);
                    leftValueItr += varCharLength;
                    resultValueItr += varCharLength;
                } else if (leftRecordDescriptor[i].type == TypeReal) {
                    memcpy(resultValueItr, leftValueItr, sizeof(float));
                    resultValueItr += sizeof(float);
                    leftValueItr += sizeof(float);
                }
            }
        }
        // now data from right record
        for (int i = 0; i < rightnumFieldsInRecord; i++) {
            if (!checkBitSet(i, rightnullIndicatorPtr)) {
                if (rightRecordDescriptor[i].type == TypeInt) {
                    memcpy(resultValueItr, rightValueItr, sizeof(int));
                    resultValueItr += sizeof(int);
                    rightValueItr += sizeof(int);
                } else if (rightRecordDescriptor[i].type == TypeVarChar) {
                    int varCharLength = *((int *) rightValueItr);
                    memcpy(resultValueItr, &varCharLength, sizeof(int));
                    rightValueItr += sizeof(int);
                    resultValueItr += sizeof(int);
                    memcpy(resultValueItr, rightValueItr, varCharLength);
                    rightValueItr += varCharLength;
                    resultValueItr += varCharLength;
                } else if (rightRecordDescriptor[i].type == TypeReal) {
                    memcpy(resultValueItr, rightValueItr, sizeof(float));
                    resultValueItr += sizeof(float);
                    rightValueItr += sizeof(float);
                }
            }
        }
        return PASS;
    }

    RC GetAttributeValueFromData(const std::vector <Attribute> recordDescriptor, const std::string &attributeName,
                                 void *inputData, void *output) {
        //get index of the required attribute
        int requiredIndex = -1;
        char *valuePtr = (char *) inputData;
        char *lhsValuePtr = (char *) output;
        char *nullIndicatorPtr = (char *) inputData;
        //sizes
        int numFieldsInRecord = recordDescriptor.size();
        int nullIndicatorSize = ceil((float) numFieldsInRecord / 8);
        valuePtr += nullIndicatorSize;
        for (int index = 0; index < recordDescriptor.size(); index++) {
            if (checkBitSet(index, nullIndicatorPtr)) {
                continue;
            }
            if (recordDescriptor[index].name == attributeName) {
                requiredIndex = index;
                break;
            }
            if (recordDescriptor[index].type == TypeInt) {
                valuePtr += sizeof(int);
            } else if (recordDescriptor[index].type == TypeVarChar) {
                int varCharLength = *((int *) valuePtr);
                valuePtr += sizeof(int) + varCharLength;
            } else if (recordDescriptor[index].type == TypeReal) {
                valuePtr += sizeof(float);
            }
        }
        if (requiredIndex == -1) {
            return FAILED;
        }

        if (!checkBitSet(requiredIndex, nullIndicatorPtr)) {
            //first filed is NULL indicator
            if (recordDescriptor[requiredIndex].type == TypeInt) {
                memcpy(lhsValuePtr, valuePtr, sizeof(int));
            } else if (recordDescriptor[requiredIndex].type == TypeReal) {
                memcpy(lhsValuePtr, valuePtr, sizeof(float));
            }
        } else {
            return MOVETONEXT;

        }
        return PASS;

    }

    void INLJoin::setUp() {
        this->leftIn->getAttributes(this->leftTableRecordDescriptor);
        this->rightIn->getAttributes(this->rightTableRecordDescriptor);
        mergeRecordDescriptors(this->leftTableRecordDescriptor, this->rightTableRecordDescriptor,
                               this->joinedTableRecordDescriptor);
        for (auto attr: this->leftTableRecordDescriptor) {
            if (attr.name.compare(this->condition.lhsAttr) == 0) {
                this->leftConditionAttribute = attr;
            }
        }
        for (auto attr: this->leftTableRecordDescriptor) {
            if (attr.name.compare(this->condition.lhsAttr) == 0) {
                this->rightCinditionAttribute = attr;
            }
        }
    }

    INLJoin::INLJoin(Iterator *leftIn, IndexScan *rightIn, const Condition &condition) {
        this->leftIn = leftIn;
        this->rightIn = rightIn;
        this->condition = condition;
        this->nodatainLeftTable = false;
        this->nodatainRightTable = true;
        this->leftRecord = malloc(PAGE_SIZE);
        this->leftExtractedValue = malloc(PAGE_SIZE);
        this->activerightRecord = malloc(PAGE_SIZE);
        setUp();
    }

    INLJoin::~INLJoin() {
        this->leftIn = NULL;
        this->rightIn = NULL;
        loadedFree(&leftRecord);
        loadedFree(&leftExtractedValue);
        loadedFree(&activerightRecord);
        leftTableRecordDescriptor.clear();
        rightTableRecordDescriptor.clear();
        joinedTableRecordDescriptor.clear();
    }

    RC INLJoin::getNextTuple(void *data) {
        int rcRight;
        if (this->nodatainLeftTable) {
            return QE_EOF;
        }

        if (!this->nodatainRightTable) {
            rcRight = this->rightIn->getNextTuple(activerightRecord);
            if (rcRight == QE_EOF) {
                this->nodatainRightTable = true;
            }
        }
        if (this->nodatainRightTable && !this->nodatainLeftTable) {
            bool breaKFlag = false;
            while (this->leftIn->getNextTuple(leftRecord) != QE_EOF) {
                GetAttributeValueFromData(this->leftTableRecordDescriptor, this->leftConditionAttribute.name,
                                          leftRecord, leftExtractedValue);

                this->rightIn->setIterator(leftExtractedValue, leftExtractedValue, true, true);

                rcRight = this->rightIn->getNextTuple(activerightRecord);
                if (rcRight == PASS) {
                    this->nodatainRightTable = false;
                    breaKFlag = true;
                    break;
                } else {
                    this->nodatainRightTable = true;
                }
            }
            if (!breaKFlag) {
                this->nodatainLeftTable = true;
            }
        }

        if (this->nodatainLeftTable && this->nodatainRightTable) {
            return QE_EOF;
        }

        mergeRecords(leftTableRecordDescriptor, rightTableRecordDescriptor, leftRecord, activerightRecord, data);
        return PASS;
    }

    RC INLJoin::getAttributes(std::vector <Attribute> &attrs) const {
        attrs = this->joinedTableRecordDescriptor;
        return PASS;
    }

    GHJoin::GHJoin(Iterator *leftIn, Iterator *rightIn, const Condition &condition, const unsigned int numPartitions) {

    }

    GHJoin::~GHJoin() {

    }

    RC GHJoin::getNextTuple(void *data) {
        return QE_EOF;
    }

    RC GHJoin::getAttributes(std::vector <Attribute> &attrs) const {
        return -1;
    }

    void Aggregate::setupAggregate() {
        this->input->getAttributes(this->aggregateRecordDescriptor);
        this->isEndReached = false;
    }

    Aggregate::Aggregate(Iterator *input, const Attribute &aggAttr, AggregateOp op) {
        this->input = input;
        this->aggAttr = aggAttr;
        this->op = op;
        setupAggregate();

    }

    Aggregate::Aggregate(Iterator *input, const Attribute &aggAttr, const Attribute &groupAttr, AggregateOp op) {

    }

    Aggregate::~Aggregate() {

    }

    RC Aggregate::aggregateGetAttributeValueFromData(const std::vector <Attribute> recordDescriptor,
                                                     const std::string &attributeName, void *inputData, void *output) {
        //get index of the required attribute
        int requiredIndex = -1;
        char *valuePtr = (char *) inputData;
        char *lhsValuePtr = (char *) output;
        char *nullIndicatorPtr = (char *) inputData;
        //sizes
        int numFieldsInRecord = recordDescriptor.size();
        int nullIndicatorSize = ceil((float) numFieldsInRecord / 8);
        valuePtr += nullIndicatorSize;
        for (int index = 0; index < recordDescriptor.size(); index++) {
            if (checkBitSet(index, nullIndicatorPtr)) {
                continue;
            }
            if (recordDescriptor[index].name == attributeName) {
                requiredIndex = index;
                break;
            }
            if (recordDescriptor[index].type == TypeInt) {
                valuePtr += sizeof(int);
            } else if (recordDescriptor[index].type == TypeVarChar) {
                int varCharLength = *((int *) valuePtr);
                valuePtr += sizeof(int) + varCharLength;
            } else if (recordDescriptor[index].type == TypeReal) {
                valuePtr += sizeof(float);
            }
        }
        if (requiredIndex == -1) {
            return FAILED;
        }

        if (!checkBitSet(requiredIndex, nullIndicatorPtr)) {
            //first filed is NULL indicator
            if (recordDescriptor[requiredIndex].type == TypeInt) {
                memcpy(lhsValuePtr, valuePtr, sizeof(int));
            } else if (recordDescriptor[requiredIndex].type == TypeReal) {
                memcpy(lhsValuePtr, valuePtr, sizeof(float));
            }
        } else {
            return MOVETONEXT;

        }
        return PASS;

    }

    RC Aggregate::getNextTuple(void *data) {
        if (this->isEndReached) {
            return QE_EOF;
        }
        void *tmpBuffer = malloc(PAGE_SIZE);
        float floatResult = 0;
        int intResult = 0;
        int count = 0;

        if (this->aggAttr.type == TypeReal) {
            if (this->op == MIN) {
                floatResult = std::numeric_limits<float>::max();
            } else if (this->op == MAX) {
                floatResult = std::numeric_limits<float>::min();
            }
        } else if (this->aggAttr.type == TypeInt) {
            if (this->op == MIN) {
                intResult = std::numeric_limits<float>::max();
            } else if (this->op == MAX) {
                intResult = std::numeric_limits<float>::min();
            }
        }

        while (this->input->getNextTuple(tmpBuffer) != QE_EOF) {
            void *extractedAtrributePtr = malloc(sizeof(int));

            int RC = aggregateGetAttributeValueFromData(this->aggregateRecordDescriptor, this->aggAttr.name, tmpBuffer,
                                                        extractedAtrributePtr);
            float floatExtractedAtrribute;
            int intExtractedAtrribute;
            if (this->aggAttr.type == TypeReal) {
                floatExtractedAtrribute = *((float *) extractedAtrributePtr);
            } else if (this->aggAttr.type == TypeInt) {
                intExtractedAtrribute = *((int *) extractedAtrributePtr);
            }

            if (RC == MOVETONEXT) {
                continue;
            } else if (RC == FAILED) {
                return FAILED;
            }

            if (this->aggAttr.type == TypeInt) {
                switch (this->op) {
                    case MIN: {
                        if (intExtractedAtrribute < intResult) {
                            intResult = intExtractedAtrribute;
                        }
                        break;
                    }
                    case MAX: {
                        if (intExtractedAtrribute > intResult) {
                            intResult = intExtractedAtrribute;
                        }
                        break;
                    }
                    case COUNT: {
                        intResult++;
                    }
                    case SUM: {
                        intResult += intExtractedAtrribute;
                        break;
                    }
                    case AVG: {
                        intResult += intExtractedAtrribute;
                        count++;
                        break;
                    }
                }
            } else {
                switch (this->op) {
                    case MIN: {
                        if (floatExtractedAtrribute < floatResult) {
                            floatResult = floatExtractedAtrribute;
                        }
                        break;
                    }
                    case MAX: {
                        if (floatExtractedAtrribute > floatResult) {
                            floatResult = floatExtractedAtrribute;
                        }
                        break;
                    }
                    case COUNT: {
                        floatResult++;
                    }
                    case SUM: {
                        floatResult += floatExtractedAtrribute;
                        break;
                    }
                    case AVG: {
                        floatResult += floatExtractedAtrribute;
                        count++;
                        break;
                    }
                }
            }
        }
        this->isEndReached = true;
        loadedFree(&tmpBuffer);
        float finalResult;
        if (this->op == AVG) {
            if (this->aggAttr.type == TypeReal) {
                finalResult = floatResult / count;
            } else if (this->aggAttr.type == TypeInt) {
                finalResult = (float) intResult / count;
            }
        } else {

            if (this->aggAttr.type == TypeReal) {
                finalResult = floatResult;
            } else if (this->aggAttr.type == TypeInt) {
                finalResult = (float) intResult;
            }
        }
        *((char *) data) = 0;
        *((float *) ((char *) data + sizeof(char))) = finalResult;
        return PASS;
    }

    RC Aggregate::getAttributes(std::vector <Attribute> &attrs) const {
        std::string opName;
        std::string resultAttrName;
        Attribute resultAttribute;
        resultAttribute.type = TypeReal;
        resultAttribute.length = sizeof(float);

        switch (this->op) {
            case MIN: {
                opName = "MIN";
                break;
            }
            case MAX: {
                opName = "MAX";
                break;
            }
            case COUNT: {
                opName = "COUNT";
                break;
            }
            case SUM: {
                opName = "SUM";
                break;
            }
            case AVG: {
                opName = "AVG";
                break;
            }
        }

        resultAttrName = opName + "(" + this->aggAttr.name + ")";
        resultAttribute.name = resultAttrName;
        attrs.clear();
        attrs.push_back(resultAttribute);
        return PASS;
    }
} // namespace PeterDB
