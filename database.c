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

#define MAX_INSERT_ARGS 2
#define SELECT_ARGS

/*
* Common Node Header Layout
*/
#define NODE_TYPE_SIZE sizeof(uint8_t)
#define NODE_TYPE_OFFSET = 0
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
#define LEAF_NODE_HEADER_SIZE (COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE)

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
    uint32_t currId;
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
Cursor *tableEnd(Table *table);
uint32_t leafNodenumCells(void *node);
void *leafNodeCell(void *node, uint32_t cellNum);
uint32_t leafNodeKey(void *node, uint32_t cellNum);
void *leafNodeValue(void *node, uint32_t cellNum);
void initialiseLeafNode(void *node);

//Program
int main(int argc, char *argv[]) {
    if (argc != 2) {
        printf("Please provide a database file or only 1 database file\n");
        exit(1);
    }
    char *fileName = argv[1];
    Table *table = databaseOpen(fileName);
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
    }  else { 
        char *command = strtok(inputBuffer->input, " ");
        if (strcmp(command, "insert") == 0) {
            doInsert(inputBuffer, table);
        } else if (strcmp(command, "select") == 0) {
            doSelect(inputBuffer, table);
        } else if (strcmp(command, "print") == 0) {
            printTable(table);
        } else {
            printf("Unrecognised Command %s\n", command);
        } 
    } 
}

void doInsert(InputBuffer *inputBuffer, Table *table) {
    void *node = getPage(table->pager, table->rootPageNum);
    if ((*(uint32_t *)leafNodenumCells(node) >= LEAF_NODE_MAX_CELLS)) {
        printf("Table is full\n");
        return;
    }
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
    rowToInsert->id = table->currId;
    strncpy(rowToInsert->userName, insert_args[0], MAX_USERNAME_SIZE);
    strncpy(rowToInsert->email, insert_args[1], MAX_EMAIL_SIZE);

    Cursor *cursor = tableEnd(table);    
    leafNodeInsert(cursor, table->currId, rowToInsert);
    table->currId += 1;
    free(rowToInsert);
    free(cursor);
    printf("Inserted Successfully\n");
}

bool insertArgsCheck(char *arg, int len) {
    if (len == 0) {
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

    uint32_t numCells = *(uint32_t *)leafNodenumCells(node);
    if (numCells >= LEAF_NODE_MAX_CELLS) {
        printf("Need to implement splitting a leaf node.\n");
        exit(EXIT_FAILURE);
    }

    if (cursor->cellNum < numCells) {
        for (uint32_t i = numCells; i > cursor->cellNum; i--) {
            memcpy(leafNodeCell(node, i), leafNodeCell(node, i - 1), LEAF_NODE_CELL_SIZE);
        }
    }

    *((uint32_t *)leafNodenumCells(node)) += 1;
    *((uint32_t *)leafNodeKey(node, cursor->cellNum)) = key;
    serialiseRow(value, leafNodeValue(node, cursor->cellNum));
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
        if (number[i] < '0' || number[i] > '9') return false;
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
    strncpy((uint32_t *)destination + USERNAME_OFFSET, source->userName, USERNAME_SIZE);
    strncpy((uint32_t *)destination + EMAIL_OFFSET, source->email, EMAIL_SIZE);
}

void deserialiseRow(void *source, Row *destination) {
    memcpy(&(destination->id), (uint32_t *)source + ID_OFFSET, ID_SIZE);
    memcpy(&(destination->userName), (uint32_t *)source + USERNAME_OFFSET, USERNAME_SIZE);
    memcpy(&(destination->email), (uint32_t *)source + EMAIL_OFFSET, EMAIL_SIZE);
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
        
        pager->pages[pageNum] = page;

        if (pageNum >= pager->numPages) {
            pager->numPages = pageNum + 1;
            // lseek(pager->fileDescriptor, pageNum * PAGE_SIZE, SEEK_SET);
            // ssize_t bytes_read = read(pager->fileDescriptor, page, PAGE_SIZE);

            // if (bytes_read == -1) {
            //     printf("Error reading file: %d\n", errno);
            //     exit(EXIT_FAILURE);
            // }
        }

    }

    return pager->pages[pageNum];    
}

Table *databaseOpen(char *fileName) {
    Pager *pager = pagerOpen(fileName);

    

    Table *table = malloc(sizeof(Table));
    table->rootPageNum = 0;
    //fix later
    table->currId = 0;
    table->pager = pager;

    if (pager->numPages == 0) {
        void *rootNode = getPage(pager, 0);
        initialiseLeafNode(rootNode);
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

Cursor *tableEnd(Table *table) {
    Cursor *cursor = malloc(sizeof(Cursor));
    cursor->table = table;
    cursor->pageNum = table->rootPageNum;

    void *rootNode = getPage(table->pager, table->rootPageNum);
    uint32_t numCells = *(uint32_t *)leafNodenumCells(rootNode);
    cursor->cellNum = numCells;
    cursor->endOfTable = true;

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
        cursor->endOfTable = true;
    }
}

uint32_t leafNodenumCells(void *node) {
    return (uint32_t *)node + LEAF_NODE_NUM_CELLS_OFFSET;
}

void *leafNodeCell(void *node, uint32_t cellNum) {
    return (uint32_t *)node + LEAF_NODE_HEADER_SIZE + cellNum * LEAF_NODE_CELL_SIZE;
}

uint32_t leafNodeKey(void *node, uint32_t cellNum) {
    return leafNodeCell(node, cellNum);
}

void *leafNodeValue(void *node, uint32_t cellNum) {
    return (uint32_t *)leafNodeCell(node, cellNum) + LEAF_NODE_KEY_SIZE;
}
void initialiseLeafNode(void *node) {
    *(uint32_t *)leafNodenumCells(node) = 0;
}