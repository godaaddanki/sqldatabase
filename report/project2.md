## Project 2 Report


### 1. Basic information
 - Team #:
   -GithubRepoLink:https://github.com/UCI-Chenli-teaching/cs222-winter22-godaaddanki
   -Student1UCINetID: gaddanki
   -Student1Name: Goda Devi Addanki

 
### 2. Meta-data
- Show your meta-data design (Tables and Columns table) and information about each column.
I followed same metadata as suggested in project-2 description.
- Tables tablehas three columns as below:
  Tables (table-id:int, table-name:varchar(50), file-name:varchar(50))
- - columns table has five columns as below:
  Columns(table-id:int, column-name:varchar(50), column-type:int, column-length:int, column-position:int)

### 3. Internal Record Format (in case you have changed from P1, please re-enter here)
- Show your record format design.
- |num of fields| null bitmap| pointer1| pointer2| pointer3| fieldvalue1| fieldvalue2| fieldvalue3|
  ->first section of record stores number of fields
  ->second section stores null bitmap
  ->third section stores pointers to the value fields. The pointer is pointed to the end of the
  field value,as the start of pointer is known from offset values and from previous pointers
  ->fourth section stores actual field values
- 
one change made to previous record design is -> if it's tombstone, only RID of next record location 
is stored in record . format -- tombstone Byte| Pagenum | Slotnum


- Describe how you store a null field.
  ->In the record,null field is not stored in the filed values section.But in the pointer section,one pointer exists
  for it, and it has same pointer value as the previous pointer,indirectly indicating that respective field is NULL



- Describe how you store a VarChar field.
  ->While reading the record,based on the type,if its varchar,first 4bytes indicate length of the varchar,
  so,based on the length,next num.of bytes are stored as varchar value,and pointer directory is updated as
  Pointers give info about where is the end of the value


- Describe how your record design satisfies O(1) field access.
  -> to access any record value, we can go to the respective pointer directly , as pointer location is based on the
  index of the field i.e 4bytes+ num fileds* bytes+ field index*4bytes.
  -> From the pointer and previous pointer, we would know where the field value is actually stored and its end pointer,
  so it's fixed time to access a field which is O(1)


### 4. Page Format (in case you have changed from P1, please re-enter here)
- Show your page format design.
- Page grows from both start and end. Records are stored in starting of the page, at the end meta data about free space
  num of slots, tombstone bytes , pointers to each record start and each record's size is stored
- The info in meta helps to figure out if a new record can be fit into the page and if so , where should it be inserted
  records are stored contiguously in page
- if a recorded is updated or deleted, all the records are shifted appropriately and meta data is updated

- Explain your slot directory design if applicable.
- Slot directory has three parameters - tombstone byte, record pointer and record size as num of bytes
- it is stored in the page end and new slot info is added as records keep adding. SLots are added 
  if current slots are not updated  after record deletion
- leftmost byte for tombstone indormationleft most 4bytes for pointer, right four bytes for length in each slot information unit

--Tombstone byte information
--Four bits are used in it to store info about if a record is tombstone and also info about if a record is
deleted
-- first bit- if its a tombstone or not
-- second,third bit - if tombstone is part of a series of tombstone,or start of the tombstone or end of the tombstone
-- fourth bit - if record is deleted or not


### 5. Page Management (in case you have changed from P1, please re-enter here)
- How many hidden pages are utilized in your design?
  -> one hidden page used to store info about the 3 counters.


- Show your hidden page(s) format design if applicable
  -> Hidden page has info about three counters - readpagecounter ,writepagecounter and appendpagecounter
  -> three counters are unsigned int type


### 6. Describe the following operation logic.
- Delete a record
-- when a record is deleted,all other records are shifted to make them contiguous
--tombstone delete bit is updated with the info
- -Slot's meta data and free space is updated



- Update a record
--to update a record, check the old and new record sizes, free size of the page to make 
   a decision about how to shift other records and whether to make is a tombstone
- -Slot's meta data is and free space is updated
- --if tombstone is created, then tombstone byte is also updated with information

- Scan on normal records
-- On normal records , scan would read the records in order from page 0 first slot to end 
-- after finding a valid record, entire record is read and information from it us used to make 
  data with required attributes. Indexes of original record descripter which matches with 
  required attributes is stored in a vector and read into data pointer.

- Scan on deleted records
for deleted records, scan would pass them and continue to next valid record 


- Scan on updated records
for updated record, scan would skip tombstone records, only when it reaches actual record, then only
 it is considered valid. tombstone byte helps to check if a record is at tombstone start 
 or end of linkedlist of tombstones

### 7. Implementation Detail
- Other implementation details goes here.



### 8. Member contribution (for team of two)
- Explain how you distribute the workload in team.
  I worked as one-person team for the project


### 9. Other (optional)
- Freely use this section to tell us about things that are related to the project 1, but not related to the other sections (optional)



- Feedback on the project to help improve the project. (optional)