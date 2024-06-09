## Project 4 Report


### 1. Basic information
- Team #:
  - GithubRepoLink:https://github.com/UCI-Chenli-teaching/cs222-winter22-godaaddanki
  - Student1UCINetID: gaddanki
  - Student1Name: Goda Devi Addanki


### 2. Catalog information about Index
- Show your catalog information about an index (tables, columns).
  - format to check if index files exist or to retrieving index files - 
  - A fixed format of index name id given to index files which is - std::string indexName = "index_" + tableNameToCheck + "_" + indexAttribute + ".idx";
  - So, when ever info about any index file is required, like existence or open an index file for operations like 
    insertentry or delete ,  existence of indexfiles for all attributes in record desciptor of a table is checked,
    by iterating through all possible index names for a particular table .In that way, if index tree is 
    present for a particular attribute would be known and can proceed accordingly 


### 3. Filter
- Describe how your filter works (especially, how you check the condition.)
  - For filter , As suggested , an assumption that "rightoperand of condition check is a value , but not a attribute"
    is used. 
  - based on type of compare operator, selection condition is made for each tuple received from input iterater.
  - for int -> check difference of A-B is positive or negetive or 0 , and assign comparater accordingly. Similarly for float
  - for varchar string compare is used.


### 4. Project
- Describe how your project works.
  - Iterating through all the given tuples from input iterator and extracting only required attributes. Must follow the order
   of input required attributes. 



### 5. Block Nested Loop Join
- Describe how your block nested loop join works (especially, how you manage the given buffers.)
  - form the block size of numPages*PAGE_SIZE from given input iterater. Feed the required key and tuple to hashmap
  - once block is ready, setup right iterater and iterate through all its records , checking for the kep to be present in hash map
  - if key is present in hashmap, then join the record.



### 6. Index Nested Loop Join
- Describe how your index nested loop join works.
  - iterate through input iterater one by one and check for the presence of key in index tree.
  - if index iterater returns tuple, merge the records.


### 7. Grace Hash Join (If you have implemented this feature)
- Describe how your grace hash join works (especially, in-memory structure).



### 8. Aggregation
- Describe how your basic aggregation works.
  - aggregation assumption is that it is valid for int and float(expect count) , so based on type of aggregate a switch case is 
   implimented within getnext of input iterater. 
  - converted to float for the final value

- Describe how your group-based aggregation works. (If you have implemented this feature)



### 9. Implementation Detail
- Have you added your own module or source file (.cc or .h)?
  Clearly list the changes on files and CMakeLists.txt, if any.
  - no extra files



- Other implementation details:



### 10. Member contribution (for team of two)
- Explain how you distribute the workload in team.
  - I worked as one-person team for the project


### 11. Other (optional)
- Freely use this section to tell us about things that are related to the project 4, but not related to the other sections (optional)



- Feedback on the project to help improve the project. (optional)