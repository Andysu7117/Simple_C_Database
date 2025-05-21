#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include <stdint.h>
#include <stddef.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>


// GLOBAL VARIABLES
#ifdef _WIN32
#include <BaseTsd.h>
typedef SSIZE_T ssize_t;
#endif

#ifndef S_IRUSR
#define S_IRUSR 00400
#endif
#ifndef S_IWUSR
#define S_IWUSR 00200
#endif

#define MAX_USERNAME_SIZE 32
#define MAX_EMAIL_SIZE 255

#define size_of_attribute(Struct, Attribute) sizeof(((Struct*)0)->Attribute)

#define ID_SIZE size_of_attribute(Row, id)
#define USERNAME_SIZE (sizeof(char) * MAX_USERNAME_SIZE)
#define EMAIL_SIZE (sizeof(char) * MAX_EMAIL_SIZE)
#define ID_OFFSET 0
#define USERNAME_OFFSET (ID_OFFSET + ID_SIZE)
#define EMAIL_OFFSET (USERNAME_OFFSET + USERNAME_SIZE)
#define ROW_SIZE (ID_SIZE + USERNAME_SIZE + EMAIL_SIZE)

#define PAGE_SIZE 4096
#define TABLE_MAX_PAGES 100
#define ROWS_PER_PAGE (PAGE_SIZE / ROW_SIZE)
#define TABLE_MAX_ROWS (ROWS_PER_PAGE * TABLE_MAX_PAGES)

#define MAX_INSERT_ARGS 3
#define SELECT_ARGS

/*
* Common Node Header Layout
*/
#define NODE_TYPE_SIZE sizeof(uint8_t)
#define NODE_TYPE_OFFSET 0
#define IS_ROOT_SIZE sizeof(uint8_t)
#define IS_ROOT_OFFSET NODE_TYPE_SIZE
#define PARENT_POINTER_SIZE sizeof(uint32_t)
#define PARENT_POINTER_OFFSET (IS_ROOT_OFFSET + IS_ROOT_SIZE)
#define COMMON_NODE_HEADER_SIZE (NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE)

/*
* Leaf Node Header Layout
*/
#define LEAF_NODE_NUM_CELLS_SIZE sizeof(uint32_t)
#define LEAF_NODE_NUM_CELLS_OFFSET COMMON_NODE_HEADER_SIZE
#define LEAF_NODE_NEXT_LEAF_SIZE sizeof(uint32_t)
#define LEAF_NODE_NEXT_LEAF_OFFSET (LEAF_NODE_NUM_CELLS_OFFSET + LEAF_NODE_NUM_CELLS_SIZE)
#define LEAF_NODE_HEADER_SIZE (COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE + LEAF_NODE_NEXT_LEAF_SIZE)

/*
* Leaf Node Body Layout
*/
#define LEAF_NODE_KEY_SIZE sizeof(uint32_t)
#define LEAF_NODE_KEY_OFFSET 0
#define LEAF_NODE_VALUE_SIZE ROW_SIZE
#define LEAF_NODE_VALUE_OFFSET (LEAF_NODE_KEY_OFFSET + LEAF_NODE_KEY_SIZE)
#define LEAF_NODE_CELL_SIZE (LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE)
#define LEAF_NODE_SPACE_FOR_CELLS (PAGE_SIZE - LEAF_NODE_HEADER_SIZE)
#define LEAF_NODE_MAX_CELLS (LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE)

#define LEAF_NODE_RIGHT_SPLIT_COUNT ((LEAF_NODE_MAX_CELLS + 1) / 2)
#define LEAF_NODE_LEFT_SPLIT_COUNT ((LEAF_NODE_MAX_CELLS + 1) - LEAF_NODE_RIGHT_SPLIT_COUNT)

/*
* Internal Node Header Layout
*/
#define INTERNAL_NODE_NUM_KEYS_SIZE sizeof(uint32_t)
#define INTERNAL_NODE_NUM_KEYS_OFFSET COMMON_NODE_HEADER_SIZE
#define INTERNAL_NODE_RIGHT_CHILD_SIZE sizeof(uint32_t)
#define INTERNAL_NODE_RIGHT_CHILD_OFFSET (INTERNAL_NODE_NUM_KEYS_OFFSET + INTERNAL_NODE_NUM_KEYS_SIZE)
#define INTERNAL_NODE_HEADER_SIZE (COMMON_NODE_HEADER_SIZE + INTERNAL_NODE_NUM_KEYS_SIZE + INTERNAL_NODE_RIGHT_CHILD_SIZE)

/*
* Internal Node Body Layout
*/
#define INTERNAL_NODE_KEY_SIZE sizeof(uint32_t)
#define INTERNAL_NODE_CHILD_SIZE sizeof(uint32_t)
#define INTERNAL_NODE_CELL_SIZE (INTERNAL_NODE_CHILD_SIZE + INTERNAL_NODE_KEY_SIZE)

// TYPEDEFS
typedef struct {
    char *input;
    size_t bufferLength;
    size_t length;
} InputBuffer;

typedef struct {
    uint32_t id;
    char userName[MAX_USERNAME_SIZE + 1];
    char email[MAX_EMAIL_SIZE + 1];
} Row;

typedef struct {
    int fileDescriptor;
    uint32_t fileLength;
    uint32_t numPages;
    void *pages[TABLE_MAX_PAGES];
} Pager;

typedef struct {
    uint32_t rootPageNum;
    Pager *pager;
} Table;

typedef struct {
    Table *table;
    uint32_t pageNum;
    uint32_t cellNum;
    bool endOfTable;
} Cursor;

// ENUM DEFINITIONS
typedef enum { INTERNAL_NODE, LEAF_NODE } NodeType;

// Function Prototypes
void printCommands();
void printPrompt();
InputBuffer *NewInputBuffer();
void readInput(InputBuffer *inputBuffer);
void closeInput(InputBuffer *InputBuffer);
ssize_t getLine(char **linePtr, size_t *n, FILE *stream);
void readAndDoCommand(InputBuffer *inputBuffer, Table *table);
void doInsert(InputBuffer *inputBuffer, Table *table);
void leafNodeInsert(Cursor *cursor, uint32_t key, Row *value);
void doSelect(InputBuffer *inputBuffer, Table *table);
bool isNumber(char *number);
bool isUserName(char *userName);
bool isEmail(char *email);
void printRow(Row *row);
void serialiseRow(Row *source, void *destination);
void deserialiseRow(void *source, Row *destination);
void *cursorValue(Cursor *cursor);
Table *databaseOpen(char *fileName);
void databaseClose(Table* table);
Pager *pagerOpen(char *filename);
void pagerFlush(Pager* pager, uint32_t pageNum);
bool insertArgsCheck(char *arg, int len);
void printTable(Table *table);
void *getPage(Pager *pager, uint32_t pageNum);
void *cursorValue(Cursor *cursor);
void cursorAdvance(Cursor *cursor);
Cursor *tableStart(Table *table);
Cursor *tableFind(Table *table, uint32_t key);
Cursor *leafNodeFind(Table *table, uint32_t pageNum, uint32_t key);
uint32_t *leafNodenumCells(void *node);
void *leafNodeCell(void *node, uint32_t cellNum);
uint32_t *leafNodeKey(void *node, uint32_t cellNum);
void *leafNodeValue(void *node, uint32_t cellNum);
void initialiseLeafNode(void *node);
void printConstants();
NodeType getNodeType(void *node);
void setNodeType(void *node, NodeType type);
uint32_t getUnusedPageNum(Pager *pager);
void createNewRoot(Table *table, uint32_t rightChildPageNum);
void initialiseInternalNode(void *node);
uint32_t *internalNodeNumKeys(void *node);
uint32_t *internalNodeRightChild(void *node);
uint32_t *internalNodeCell(void *node, uint32_t cellNum);
uint32_t *internalNodeChild(void *node, uint32_t childNum);
uint32_t *internalNodeKey(void *node, uint32_t keyNum);
uint32_t getNodeMaxKey(void *node);
bool isNodeRoot(void *node);
void setNodeRoot(void *node, bool isRoot);
void indent(uint32_t level);
void printTree(Pager *pager, uint32_t pageNum, uint32_t indentationLevel);
void leafNodeSplitAndInsert(Cursor *cursor, uint32_t key, Row *value);
Cursor *internalNodeFind(Table *table, uint32_t pageNum, uint32_t key);
uint32_t *leafNodeNextLeaf(void *node);

//Program
int main(int argc, char *argv[]) {
    if (argc != 2) {
        printf("Please provide a database file or only 1 database file\n");
        exit(1);
    }
    char *fileName = argv[1];
    Table *table = databaseOpen(fileName);
    printConstants();
    printCommands();
    InputBuffer *inputBuffer = NewInputBuffer();
    while (true) {
        printPrompt();
        readInput(inputBuffer);
        readAndDoCommand(inputBuffer, table);
    }

    return 0;
}

// Prints all commands
void printCommands() {
    printf("Commands are\n");
    printf("help: prints all commands\n");
    printf("insert: To insert data input as 'insert <username> <email>'\n");
    printf("delete: To delete data 'delete id/username/email'\n");
    printf("modify: To modify data 'modify username/email <username/email> <newusername/newemail>'\n");
    printf("select: To select data 'select <row>'\n");
    printf("tree: prints the bst\n");
    printf("print: Prints all data\n");
    printf("exit: Exits program\n");
    printf("\n");
}

// Prints command prompt insert buffer
void printPrompt() {
    printf("db > ");
}

// Creates new input buffer
InputBuffer *NewInputBuffer() {
    InputBuffer *newInputBuffer = malloc(sizeof(InputBuffer));
    newInputBuffer->input = NULL;
    newInputBuffer->bufferLength = 0;
    newInputBuffer->length = 0;

    return newInputBuffer;
}

// Reads user input
void readInput(InputBuffer *inputBuffer) {
    ssize_t bytesRead = getLine(&(inputBuffer->input), &(inputBuffer->bufferLength), stdin);

    if (bytesRead <= 0) {
        printf("Error Reading Input");
        exit(1);
    }
}

ssize_t getLine(char **linePtr, size_t *n, FILE *stream) {
    size_t chunk = 128;
    size_t pos = 0;
    int c;

    if (*linePtr == NULL || *n == 0) {
        *n = chunk;
        *linePtr = malloc(*n * sizeof(char));
        if (*linePtr == NULL) return -1;
    }

    while ((c = fgetc(stream)) != EOF) {
        if (pos + 1 >= *n) {
            *n += chunk;
            char *newPtr = realloc(*linePtr, *n * sizeof(char));
            if (!newPtr) return -1;
            *linePtr = newPtr;
        }

        (*linePtr)[pos++] = c;
        if (c == '\n') break;
    }

    if (pos == 0 && c == EOF) return -1;

    // ignore newline
    (*linePtr)[pos - 1] = '\0';
    return pos - 1;
}


void closeInput(InputBuffer *inputBuffer) {
    free(inputBuffer->input);
    free(inputBuffer);
}

void readAndDoCommand(InputBuffer *inputBuffer, Table *table) {   
    if (strcmp(inputBuffer->input, "exit") == 0) {
        printf("Closing...\n");
        closeInput(inputBuffer);
        databaseClose(table);
        printf("Successfully closed\n");
        exit(EXIT_SUCCESS);
    } else if (strcmp(inputBuffer->input, "help") == 0) {
        printCommands();
    }  else if (strcmp(inputBuffer->input, "tree") == 0) {
        printTree(table->pager, 0, 0);
    }  else if (strcmp(inputBuffer->input, "print") == 0) {
        printTable(table);
    } else { 
        char *command = strtok(inputBuffer->input, " ");
        if (strcmp(command, "insert") == 0) {
            doInsert(inputBuffer, table);
        } else if (strcmp(command, "select") == 0) {
            doSelect(inputBuffer, table);
        } else {
            printf("Unrecognised Command %s\n", command);
        } 
    } 
}

void doInsert(InputBuffer *inputBuffer, Table *table) {
    void *node = getPage(table->pager, table->rootPageNum);
    uint32_t numCells = (*leafNodenumCells(node));

    char *insert_args[MAX_INSERT_ARGS + 1] = { NULL };

    int len = 0;
    for (char *arg; (arg = strtok(NULL, " ")) && len < MAX_INSERT_ARGS;) {
        if (!insertArgsCheck(arg, len)) {
            return;
        }
        insert_args[len] = arg;
        len++;
    }

    Row *rowToInsert = malloc(sizeof(Row));
    rowToInsert->id = atoi(insert_args[0]);
    strncpy(rowToInsert->userName, insert_args[1], MAX_USERNAME_SIZE);
    strncpy(rowToInsert->email, insert_args[2], MAX_EMAIL_SIZE);

    uint32_t keyToInsert = rowToInsert->id;
    Cursor *cursor = tableFind(table, keyToInsert);
    
    if (cursor->cellNum < numCells) {
        uint32_t keyAtIndex = *leafNodeKey(node, cursor->cellNum);
        if (keyAtIndex == keyToInsert) {
            printf("Duplicate Record Found\n");
            return;
        }
    }

    leafNodeInsert(cursor, keyToInsert, rowToInsert);
    free(rowToInsert);
    free(cursor);
    printf("Inserted Successfully\n");
}

bool insertArgsCheck(char *arg, int len) {
    if (len == 0) {
        if (!isNumber(arg)) {
            printf("Invalid id\n");
            return false;
        }
    } else if (len == 1) {
        if (!isUserName(arg)) {
            printf("Invalid username\n");
            return false;
        }
    } else {
        if (!isEmail(arg)) {
            printf("Invalid email\n");
            return false;
        }
    }

    return true;
}

void leafNodeInsert(Cursor *cursor, uint32_t key, Row *value) {
    void *node = getPage(cursor->table->pager, cursor->pageNum);

    uint32_t numCells = *leafNodenumCells(node);
    if (numCells >= LEAF_NODE_MAX_CELLS) {
        printf("I got here\n");
        leafNodeSplitAndInsert(cursor, key, value);
        return;
    }

    if (cursor->cellNum < numCells) {
        for (uint32_t i = numCells; i > cursor->cellNum; i--) {
            memcpy(leafNodeCell(node, i), leafNodeCell(node, i - 1), LEAF_NODE_CELL_SIZE);
        }
    }

    *leafNodenumCells(node) += 1;
    *((uint32_t *)leafNodeKey(node, cursor->cellNum)) = key;
    serialiseRow(value, leafNodeValue(node, cursor->cellNum));
}

void leafNodeSplitAndInsert(Cursor *cursor, uint32_t key, Row *value) {
    void *prevNode = getPage(cursor->table->pager, cursor->pageNum);
    uint32_t prevMax = getNodeMaxKey(prevNode);
    uint32_t newPageNum = getUnusedPageNum(cursor->table->pager);
    void *newNode = getPage(cursor->table->pager, newPageNum);
    initialiseLeafNode(newNode);
    *nodeParent(newNode) = *nodeParent(prevNode);
    *leafNodeNextLeaf(newNode) = *leafNodeNextLeaf(prevNode);
    *leafNodeNextLeaf(prevNode) = newPageNum;

    for (int32_t i = LEAF_NODE_MAX_CELLS; i >= 0; i--) {
        void *destNode;
        if (i >= LEAF_NODE_LEFT_SPLIT_COUNT) {
            destNode = newNode;
        } else {
            destNode = prevNode;
        }
        uint32_t indexWithinNode = i % LEAF_NODE_LEFT_SPLIT_COUNT;
        void *destination = leafNodeCell(destNode, indexWithinNode);

        if (i == cursor->cellNum) {
            serialiseRow(value, leafNodeValue(destNode, indexWithinNode));
        } else if (i > cursor->cellNum) {
            memcpy(destination, leafNodeCell(prevNode, i - 1), LEAF_NODE_CELL_SIZE);
        } else {
            memcpy(destination, leafNodeCell(prevNode, i), LEAF_NODE_CELL_SIZE);
        }
    }

    *(leafNodenumCells(prevNode)) = LEAF_NODE_LEFT_SPLIT_COUNT;
    *(leafNodenumCells(newNode)) = LEAF_NODE_RIGHT_SPLIT_COUNT;

    if (isNodeRoot(prevNode)) {
        createNewRoot(cursor->table, newPageNum);
    } else {
        uint32_t parentPageNum = *nodeParent(prevNode);
        uint32_t newMax = getNodeMaxKey(prevNode);
        void *parent = getPage(cursor->table->pager, parentPageNum);

        updateInternalNodeKey(parent, prevMax, newMax);
        internalNodeInsert(cursor->table, parentPageNum, newPageNum);
        return;
    }
}

Cursor *internalNodeFind(Table *table, uint32_t pageNum, uint32_t key) {
    void *node = getPage(table->pager, pageNum);
    uint32_t numKeys = *internalNodeNumKeys(node);

    uint32_t minIndex = 0;
    uint32_t maxIndex = numKeys;

    while (minIndex != maxIndex) {
        uint32_t index = (minIndex + maxIndex) / 2;
        uint32_t keyToRight = *internalNodeKey(node, index);

        if (keyToRight >= key) {
            maxIndex = index;
        } else {
            minIndex = index + 1;
        }
    }

    uint32_t childNum = *internalNodeChild(node, minIndex);
    void *child = getPage(table->pager, childNum);
    NodeType type = getNodeType(child);

    if (type == LEAF_NODE) {
        return leafNodeFind(table, childNum, key);
    } else {
        return internalNodeFind(table, childNum, key);
    }
}

void doSelect(InputBuffer *inputBuffer, Table *table) {
    char *selectedRow = strtok(NULL, " ");
    if (!isNumber(selectedRow)) {
        printf("Invalid row\n");
        return;
    }
    uint32_t rowSelected = atoi(selectedRow);
    // if (!validRow(rowSelected, table)) {
    //     printf("Row selected is too large\n");
    //     return;
    // }

    Row row;
    Cursor *cursor = tableStart(table);
    deserialiseRow(cursorValue(cursor), &row);
    printRow(&row);

    free(cursor);
}


bool isNumber(char *number) {
    if (number == NULL || *number == '\0') return false;
    for (int i = 0; number[i]; i++) {
        if (number[i] < '0' || number[i] > '9') {
            printf("invalid number\n");
            return false;
        }
    }

    return true;
}

bool isUserName(char *userName) {
    if ((strlen(userName) <= 5) || (strlen(userName) > MAX_USERNAME_SIZE)) {
        return false;
    }

    return true;
}

bool isEmail(char *email) {
    if ((strstr(email, "@") == NULL) || (strlen(email) > MAX_EMAIL_SIZE)) {
        return false;
    }

    return true;
}

void printRow(Row *row) {
    printf("id: %d, username: %s, email: %s\n", row->id, row->userName, row->email);
}

void serialiseRow(Row *source, void *destination) {
    memcpy((uint32_t *)destination + ID_OFFSET, &(source->id), ID_SIZE);
    strncpy((char *)destination + USERNAME_OFFSET, source->userName, USERNAME_SIZE);
    strncpy((char *)destination + EMAIL_OFFSET, source->email, EMAIL_SIZE);
}

void deserialiseRow(void *source, Row *destination) {
    memcpy(&(destination->id), (uint32_t *)source + ID_OFFSET, ID_SIZE);
    memcpy(&(destination->userName), (char *)source + USERNAME_OFFSET, USERNAME_SIZE);
    memcpy(&(destination->email), (char *)source + EMAIL_OFFSET, EMAIL_SIZE);
}

void *getPage(Pager *pager, uint32_t pageNum) {
    if (pageNum > TABLE_MAX_PAGES) {
        printf("Tried to fetch page number out of bounds. %d > %d\n", pageNum,
            TABLE_MAX_PAGES);
        exit(EXIT_FAILURE);
    }

    if (pager->pages[pageNum] == NULL) {
        // Cache miss. Allocate memory and load from file.
        void* page = malloc(PAGE_SIZE);

        if (page == NULL) {
            printf("Error allocating memory\n");
            exit(EXIT_FAILURE);
        }

        uint32_t numPagesInFile = pager->fileLength / PAGE_SIZE;
        if (pageNum < numPagesInFile) {
            lseek(pager->fileDescriptor, pageNum * PAGE_SIZE, SEEK_SET);
            ssize_t bytes_read = read(pager->fileDescriptor, page, PAGE_SIZE);

            if (bytes_read == -1) {
                printf("Error reading file: %d\n", errno);
                exit(EXIT_FAILURE);
            }
        } else {
            memset(page, 0, PAGE_SIZE);
        }

        pager->pages[pageNum] = page;

        if (pageNum >= pager->numPages) {
            pager->numPages = pageNum + 1;
        }
    }

    return pager->pages[pageNum];    
}


Table *databaseOpen(char *fileName) {
    Pager *pager = pagerOpen(fileName);

    Table *table = malloc(sizeof(Table));
    table->rootPageNum = 0;
    table->pager = pager;

    if (pager->numPages == 0) {
        void *rootNode = getPage(pager, 0);
        initialiseLeafNode(rootNode);
        setNodeRoot(rootNode, true);
    }

    return table;
}

void databaseClose(Table* table) {
    Pager* pager = table->pager;

    for (uint32_t i = 0; i < pager->numPages; i++) {
        if (pager->pages[i] == NULL) {
        continue;
        }
        pagerFlush(pager, i);
        free(pager->pages[i]);
        pager->pages[i] = NULL;
    }

    int result = close(pager->fileDescriptor);
    if (result == -1) {
        printf("Error closing db file.\n");
        exit(EXIT_FAILURE);
    }
    for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
        void* page = pager->pages[i];
        if (page) {
        free(page);
        pager->pages[i] = NULL;
        }
    }
    free(pager);
    free(table);
}

Pager *pagerOpen(char *filename) {
    int fd = open(filename, O_RDWR | O_CREAT, S_IWUSR | S_IRUSR);

    if (fd == -1) {
        printf("Unable to open file\n");
        exit(EXIT_FAILURE);
    }

    off_t fileLength = lseek(fd, 0, SEEK_END);

    Pager *pager = malloc(sizeof(Pager));
    pager->fileDescriptor = fd;
    pager->fileLength = fileLength;
    pager->numPages = (fileLength / PAGE_SIZE);

    if (fileLength % PAGE_SIZE != 0) {
        printf("Db file is not a whole number of pages. Corrupt file\n");
        exit(EXIT_FAILURE);
    }
    
    for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
        pager->pages[i] = NULL;
    }

    return pager;
}

void pagerFlush(Pager* pager, uint32_t pageNum) {
    if (pager->pages[pageNum] == NULL) {
        printf("Tried to flush null page\n");
        exit(EXIT_FAILURE);
    }

    off_t offset = lseek(pager->fileDescriptor, pageNum * PAGE_SIZE, SEEK_SET);

    if (offset == -1) {
        printf("Error seeking: %d\n", errno);
        exit(EXIT_FAILURE);
    }

    ssize_t bytesWritten = write(pager->fileDescriptor, pager->pages[pageNum], PAGE_SIZE);

    if (bytesWritten == -1) {
        printf("Error writing: %d\n", errno);
        exit(EXIT_FAILURE);
  }
}

void printTable(Table *table) {
    Cursor *cursor = tableStart(table);
    Row row;
    while (!(cursor->endOfTable)) {
        deserialiseRow(cursorValue(cursor), &row);
        printRow(&row);
        cursorAdvance(cursor);
    }
} 

Cursor *tableStart(Table *table) {
    Cursor *cursor = malloc(sizeof(Cursor));
    cursor->table = table;
    cursor->pageNum = table->rootPageNum;
    cursor->cellNum = 0;

    void *rootNode = getPage(table->pager, table->rootPageNum);
    uint32_t numCells = *(uint32_t *)leafNodenumCells(rootNode);
    cursor->endOfTable = (numCells == 0);

    return cursor;
}

Cursor *tableFind(Table *table, uint32_t key) {
    uint32_t rootPageNum = table->rootPageNum;
    void *rootNode = getPage(table->pager, rootPageNum);

    if (getNodeType(rootNode) == LEAF_NODE) {
        return leafNodeFind(table, rootPageNum, key);
    } else {
        return internalNodeFind(table, rootPageNum, key);
    }
}

Cursor *leafNodeFind(Table *table, uint32_t pageNum, uint32_t key) {
    void *node = getPage(table->pager, pageNum);
    uint32_t numCells = *leafNodenumCells(node);

    Cursor *cursor = malloc(sizeof(Cursor));
    cursor->table = table;
    cursor->pageNum = pageNum;

    uint32_t minIndex = 0;
    uint32_t onePastMax = numCells;

    while (onePastMax != minIndex) {
        uint32_t index = (minIndex + onePastMax) / 2;
        uint32_t keyAtIndex = *leafNodeKey(node, index);
        if (key == keyAtIndex) {
            cursor->cellNum = index;
            return cursor;
        }

        if (key < keyAtIndex) {
            onePastMax = index;
        } else {
            minIndex = index + 1;
        }
    }
    cursor->cellNum = minIndex;
    return cursor;
}

void *cursorValue(Cursor *cursor) {
    uint32_t pageNum = cursor->pageNum;

    void *page = getPage(cursor->table->pager, pageNum);   

    return leafNodeValue(page, cursor->cellNum);
}

void cursorAdvance(Cursor *cursor) {
    uint32_t pageNum = cursor->pageNum;
    void *node = getPage(cursor->table->pager, pageNum);

    cursor->cellNum += 1;
    if (cursor->cellNum >= (*(uint32_t *)leafNodenumCells(node))) {
        uint32_t nextPageNum = *leafNodeNextLeaf(node);
        if (nextPageNum == 0) {
            cursor->endOfTable = true;
        } else {
            cursor->pageNum = nextPageNum;
            cursor->cellNum = 0;
        }
    }
}

uint32_t *leafNodeNextLeaf(void *node) {
    return ((uint32_t *)node + LEAF_NODE_NEXT_LEAF_OFFSET);
}

uint32_t *leafNodenumCells(void *node) {
    return (uint32_t *)node + LEAF_NODE_NUM_CELLS_OFFSET;
}

void *leafNodeCell(void *node, uint32_t cellNum) {
    return (uint32_t *)node + LEAF_NODE_HEADER_SIZE + cellNum * LEAF_NODE_CELL_SIZE;
}

uint32_t *leafNodeKey(void *node, uint32_t cellNum) {
    return leafNodeCell(node, cellNum);
}

void *leafNodeValue(void *node, uint32_t cellNum) {
    return (uint32_t *)leafNodeCell(node, cellNum) + LEAF_NODE_KEY_SIZE;
}

void initialiseLeafNode(void *node) {
    setNodeType(node, LEAF_NODE);
    *leafNodenumCells(node) = 0;
    *leafNodeNextLeaf(node) = 0;
}

void indent(uint32_t level) {
    for (uint32_t i = 0; i < level; i++) {
        printf("  ");
    }
}

void printTree(Pager *pager, uint32_t pageNum, uint32_t indentationLevel) {
    void *node = getPage(pager, pageNum);
    uint32_t numKeys, child;

    NodeType type = getNodeType(node);
    printf("Debug: pageNum=%d, node type=%d\n", pageNum, type);

    if (type == LEAF_NODE) {
        numKeys = *leafNodenumCells(node);
        printf("Debug: leaf node with %d keys\n", numKeys);
        indent(indentationLevel);
        printf("- leaf (size %d)\n", numKeys);

        for (uint32_t i = 0; i < numKeys; i++) {
            indent(indentationLevel + 1);
            printf("- %d\n", *leafNodeKey(node, i));
        }
    } else if (type == INTERNAL_NODE) {
        numKeys = *internalNodeNumKeys(node);
        indent(indentationLevel);
        printf("- internal (size %d)\n", numKeys);
        for (uint32_t i = 0; i < numKeys; i++) {
            child = *internalNodeChild(node, i);
            printTree(pager, child, indentationLevel + 1);

            indent(indentationLevel + 1);
            printf("- key %d\n", *internalNodeKey(node, i));
        }

        child = *internalNodeRightChild(node);
        printTree(pager, child, indentationLevel + 1);
    }
}

void printConstants() {
    printf("Row Size: %d\n", ROW_SIZE);
    printf("Common Node Header Size: %d\n", COMMON_NODE_HEADER_SIZE);
    printf("Leaf NodeHeader Size: %d\n", LEAF_NODE_HEADER_SIZE);
    printf("Leaf Node Cell Size: %d\n", LEAF_NODE_CELL_SIZE);
    printf("Leaf Node Space for Cells: %d\n", LEAF_NODE_SPACE_FOR_CELLS);
    printf("Leaf Node Max Cells: %d\n", LEAF_NODE_MAX_CELLS);
}

NodeType getNodeType(void *node) {
    uint8_t value = *((uint8_t *)((uint8_t *)node + NODE_TYPE_OFFSET));
    return (NodeType)value;
}

void setNodeType(void *node, NodeType type) {
    uint8_t value = type;
    *((uint8_t *)((uint8_t *)node + NODE_TYPE_OFFSET)) = value;
}

uint32_t getUnusedPageNum(Pager *pager) {
    return pager->numPages;
}

void createNewRoot(Table *table, uint32_t rightChildPageNum) {
    void *root = getPage(table->pager, table->rootPageNum);
    void *rightChild = getPage(table->pager, rightChildPageNum);
    uint32_t leftChildPageNum = getUnusedPageNum(table->pager);
    void *leftChild = getPage(table->pager, leftChildPageNum);

    memcpy(leftChild, root, PAGE_SIZE);
    setNodeRoot(leftChild, false);
    initialiseInternalNode(root);
    setNodeRoot(root, true);
    *internalNodeNumKeys(root) = 1;
    *internalNodeChild(root, 0) = leftChildPageNum;
    uint32_t leftChildMaxKey = getNodeMaxKey(leftChild);
    *internalNodeKey(root, 0) = leftChildMaxKey;
    *internalNodeRightChild(root) = rightChildPageNum; 
}

void initialiseInternalNode(void *node) {
    setNodeType(node, INTERNAL_NODE);
    setNodeRoot(node, false);
    *internalNodeNumKeys(node) = 0;
}

uint32_t *internalNodeNumKeys(void *node) {
    return ((uint32_t *)node + INTERNAL_NODE_NUM_KEYS_OFFSET);
}

uint32_t *internalNodeRightChild(void *node) {
    return ((uint32_t *)node + INTERNAL_NODE_RIGHT_CHILD_OFFSET);
}

uint32_t *internalNodeCell(void *node, uint32_t cellNum) {
    return ((uint32_t *)node + INTERNAL_NODE_HEADER_SIZE + cellNum * INTERNAL_NODE_CELL_SIZE);
}

uint32_t *internalNodeChild(void *node, uint32_t childNum) {
    uint32_t numKeys = *internalNodeNumKeys(node);
    if (childNum > numKeys) {
        printf("Tried to access childNum %d > numKeys %d\n", childNum, numKeys);
        exit(EXIT_FAILURE);
    } else if (childNum == numKeys) {
        return internalNodeRightChild(node);
    } else {
        return internalNodeCell(node, childNum);
    }
}

uint32_t *internalNodeKey(void *node, uint32_t keyNum) {
    return internalNodeCell(node, keyNum) + INTERNAL_NODE_RIGHT_CHILD_SIZE;
}

uint32_t getNodeMaxKey(void *node) {
    NodeType type = getNodeType(node);

    if (type == INTERNAL_NODE) {
        return *internalNodeKey(node, *internalNodeNumKeys(node) - 1);
    } else if (type == LEAF_NODE) {
        return *leafNodeKey(node, *leafNodenumCells(node) - 1);
    }
}

bool isNodeRoot(void *node) {
    uint8_t value = *((uint8_t *)((uint8_t *)node + IS_ROOT_OFFSET));
    return (value != 0);
}

void setNodeRoot(void *node, bool isRoot) {
    uint8_t value = isRoot;
    *((uint8_t *)((uint8_t *)node + IS_ROOT_OFFSET)) = value;
}

