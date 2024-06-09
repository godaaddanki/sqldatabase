#include <iostream>
#include "src/include/pfm.h"

namespace PeterDB {
    PagedFileManager &PagedFileManager::instance() {
        static PagedFileManager _pf_manager = PagedFileManager();
        return _pf_manager;
    }

    PagedFileManager::PagedFileManager() = default;

    PagedFileManager::~PagedFileManager() = default;

    PagedFileManager::PagedFileManager(const PagedFileManager &) = default;

    PagedFileManager &PagedFileManager::operator=(const PagedFileManager &) = default;

    RC PagedFileManager::createFile(const std::string &fileName) {
       FILE * file = fopen(fileName.c_str(),"r");
        if(!file){
            FILE * newFile;
            newFile = fopen(fileName.c_str(), "w+b");
            if (newFile) {
                FileHandle new_filehandle;
                new_filehandle.init(newFile);
                new_filehandle.closeActivefile();
                return 0;
            }
            //printf("file is NULL");
                return -1;
        }
        else{
        fclose(file);
        //printf("file already exists");
        }
        return -1;

    }

    RC PagedFileManager::destroyFile(const std::string &fileName) {
        FILE * file = fopen(fileName.c_str(),"r+b");
        if (file) {
            fclose(file);
            if (!remove(fileName.c_str())) {
                return 0;
            }
//            printf("unable to remove file");
            return -1;
        }
//        printf("file is NULL");
        return -1;
    }

    RC PagedFileManager::openFile(const std::string &fileName, FileHandle &fileHandle) {
        FILE *file = fopen(fileName.c_str(),"r+b");
        if (file) {
            fileHandle.activeFile = file;
            fileHandle.readStaticCounter();
            return 0;
        }
//        printf("file is NULL");
        return -1;
    }

    RC PagedFileManager::closeFile(FileHandle &fileHandle) {
        fileHandle.closeActivefile();
        return 0;
    }

    FileHandle::FileHandle() {
        readPageCounter = 0;
        writePageCounter = 0;
        appendPageCounter = 0;
        //pageNumCount = 0;
        activeFile = NULL;
    }

    FileHandle::~FileHandle() {
        readPageCounter = 0;
        writePageCounter = 0;
        appendPageCounter = 0;
        //pageNumCount = 0;
        activeFile = NULL;
    }

    void FileHandle::init(FILE * newfile){
        activeFile = newfile;
        hiddenPage();
    }
    RC FileHandle::readStaticCounter(){
        if (activeFile != NULL) {
            fseek(activeFile, sizeof(unsigned) * READ_PAGE_COUNT, SEEK_SET);
            int read_size = fread(&readPageCounter, sizeof(unsigned), 1, activeFile);
            fseek(activeFile, sizeof(unsigned) * WRITE_PAGE_COUNT, SEEK_SET);
            int write_size = fread(&writePageCounter, sizeof(unsigned), 1, activeFile);
            fseek(activeFile, sizeof(unsigned) * APPEND_PAGE_COUNT, SEEK_SET);
            int append_size = fread(&appendPageCounter, sizeof(unsigned), 1, activeFile);
            if (read_size && write_size && append_size){
                return 0;
            }
            return -1;

        }
        return -1;

    }
    RC FileHandle::writeStaticCounter(){
        if (activeFile != NULL) {
            fseek(activeFile, sizeof(unsigned) * READ_PAGE_COUNT, SEEK_SET);
            int read_size = fwrite(&readPageCounter, sizeof(unsigned), 1, activeFile);
            fseek(activeFile, sizeof(unsigned) * WRITE_PAGE_COUNT, SEEK_SET);
            int write_size = fwrite(&writePageCounter, sizeof(unsigned), 1, activeFile);
            fseek(activeFile, sizeof(unsigned) * APPEND_PAGE_COUNT, SEEK_SET);
            int append_size = fwrite(&appendPageCounter, sizeof(unsigned), 1, activeFile);
            //fseek(activeFile, sizeof(unsigned) * PAGE_COUNT, SEEK_SET);
            //int page_size = fwrite(&pageNumCount, sizeof(unsigned), 1, activeFile);
            if (read_size && write_size && append_size) {
                return 0;
            }
        }
        return -1;
    }

    RC FileHandle::readPage(PageNum pageNum, void *data) {
        if (activeFile != NULL && getNumberOfPages() >= pageNum + 1) {
            fseek(activeFile, PAGE_SIZE * (pageNum + 1), SEEK_SET);
            int size = fread(data, PAGE_SIZE, 1, activeFile);
//            printf("size read %d",size);
            readPageCounter++;
            return 0;
        }
//        printf("either file doesnt exist or pagenum to read is invalid\n");
        return -1;
    }

    RC FileHandle::writePage(PageNum pageNum, const void *data) {
        if (activeFile != NULL && getNumberOfPages() >= pageNum + 1) {
            fseek(activeFile, PAGE_SIZE * (pageNum + 1), SEEK_SET);
            int size = fwrite(data, PAGE_SIZE, 1, activeFile);
            writePageCounter++;
            return 0;
        }
//        printf("either file doesnt exist or pagenum to write is invalid\n");
        return -1;
    }

    RC FileHandle::appendPage(const void *data) {
        if (activeFile != NULL) {
            fseek(activeFile, 0, SEEK_END);
            fwrite(data, PAGE_SIZE, 1, activeFile);
            appendPageCounter++;
            return 0;
        }
//        printf("page size is not appended correctly\n");
        return -1;
    }

    unsigned FileHandle::getNumberOfPages() {
        if (activeFile != NULL) {
            fseek(activeFile, 0, SEEK_END);
            int page_count = ftell(activeFile) / PAGE_SIZE;
//            if (page_count >0){
//                printf("no.of pages without hidden page %d\n",page_count-1);
                return appendPageCounter;
//            }
//            printf("no.of pages %d\n",page_count);
            return page_count;
        }

        return -1;
    }

    RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
        readPageCount = readPageCounter;
        writePageCount = writePageCounter;
        appendPageCount = appendPageCounter;
//        std::cout<<"readpage : "<<readPageCount<<" writepage : "<<writePageCount<<" appendpage : "<<appendPageCount<<std::endl;
        return 0;
    }
    RC FileHandle::hiddenPage(){
        void *page = calloc(1,PAGE_SIZE);
        fseek(activeFile , 0, SEEK_SET );
        int write_size = fwrite(page, PAGE_SIZE, 1, activeFile);
        free(page);
        page = NULL;
        if (write_size == PAGE_SIZE){
            return 0;
        }
        return -1;

    }
    RC FileHandle::closeActivefile(){
        if(activeFile != NULL) {
            writeStaticCounter();
            fclose(activeFile);
            activeFile = NULL;
            return 0;
        }
        return -1;
    }
    void loadedFree(void** ptrPtr){
        if(*ptrPtr){
            free(*ptrPtr);
        }
        *ptrPtr = NULL;
    }
} // namespace PeterDB