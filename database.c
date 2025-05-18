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
    void *pages[TABLE_MAX_PAGES];
} Pager;

typedef struct {
    uint32_t noRows;
    uint32_t currId;
    Pager *pager;
} Table;

typedef struct {
    Table *table;
    uint32_t rowNum;
    bool endOfTable;
} Cursor;

// ENUM DEFINITIONS


// Function Prototypes
void printCommands();
void printPrompt();
InputBuffer *NewInputBuffer();
void readInput(InputBuffer *inputBuffer);
void closeInput(InputBuffer *InputBuffer);
ssize_t getLine(char **linePtr, size_t *n, FILE *stream);
void readAndDoCommand(InputBuffer *inputBuffer, Table *table);
void doInsert(InputBuffer *inputBuffer, Table *table);
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
void pagerFlush(Pager* pager, uint32_t pageNum, uint32_t size);
bool insertArgsCheck(char *arg, int len);
bool validRow(uint32_t selectedRow, Table *table);
void printTable(Table *table);
void *getPage(Pager *pager, uint32_t pageNum);


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
    serialiseRow(rowToInsert, cursorValue(cursor));
    table->noRows += 1;
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

void doSelect(InputBuffer *inputBuffer, Table *table) {
    char *selectedRow = strtok(NULL, " ");
    if (!isNumber(selectedRow)) {
        printf("Invalid row\n");
        return;
    }
    uint32_t rowSelected = atoi(selectedRow);
    if (!validRow(rowSelected, table)) {
        printf("Row selected is too large\n");
        return;
    }

    Row row;
    Cursor *cursor = tableStart(table);
    cursor->rowNum = rowSelected;
    deserialiseRow(cursorValue(cursor), &row);
    printRow(&row);
    
    free(cursor);
}

bool validRow(uint32_t selectedRow, Table *table) {
    if (selectedRow >= table->noRows) {
        return false;
    }

    return true;
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
    memcpy((char *)destination + ID_OFFSET, &(source->id), ID_SIZE);
    strncpy((char *)destination + USERNAME_OFFSET, source->userName, USERNAME_SIZE);
    strncpy((char *)destination + EMAIL_OFFSET, source->email, EMAIL_SIZE);
}

void deserialiseRow(void *source, Row *destination) {
    memcpy(&(destination->id), (char *)source + ID_OFFSET, ID_SIZE);
    memcpy(&(destination->userName), (char *)source + USERNAME_OFFSET, USERNAME_SIZE);
    memcpy(&(destination->email), (char *)source + EMAIL_OFFSET, EMAIL_SIZE);
}

void *cursorValue(Cursor *cursor) {
    uint32_t rowNum = cursor->rowNum;
    uint32_t pageNum = rowNum / ROWS_PER_PAGE;

    void *page = getPage(cursor->table->pager, pageNum);
    uint32_t rowOffset = rowNum % ROWS_PER_PAGE;
    uint32_t byteOffset = rowOffset * ROW_SIZE;   

    return ((char *)page + byteOffset);
}

void cursorAdvance(Cursor *cursor) {
    cursor->rowNum += 1;
    if (cursor->rowNum >= cursor->table->noRows) {
        cursor->endOfTable = true;
    }
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
        uint32_t numPages = pager->fileLength / PAGE_SIZE;

        // We might save a partial page at the end of the file
        if (pager->fileLength % PAGE_SIZE) {
            numPages += 1;
        }

        if (pageNum <= numPages) {
            lseek(pager->fileDescriptor, pageNum * PAGE_SIZE, SEEK_SET);
            ssize_t bytes_read = read(pager->fileDescriptor, page, PAGE_SIZE);

            if (bytes_read == -1) {
                printf("Error reading file: %d\n", errno);
                exit(EXIT_FAILURE);
            }
        }

        pager->pages[pageNum] = page;
    }

    return pager->pages[pageNum];    
}

Table *databaseOpen(char *fileName) {
    Pager *pager = pagerOpen(fileName);
    uint32_t numRows = pager->fileLength / ROW_SIZE;

    void *rowBuffer = malloc(ROW_SIZE);
    Row tempRow;
    int currentMaxId = 0;

    for (uint32_t i = 0; i < numRows; i++) {
        lseek(pager->fileDescriptor, i * ROW_SIZE, SEEK_SET);
        ssize_t bytesRead = read(pager->fileDescriptor, rowBuffer, ROW_SIZE);
        if (bytesRead != ROW_SIZE) {
            printf("Error reading row during pager initialization\n");
            exit(EXIT_FAILURE);
        }

        deserialiseRow(rowBuffer, &tempRow);
        if (tempRow.id > currentMaxId) {
            currentMaxId = tempRow.id;
        }
    }

    free(rowBuffer);

    Table *table = malloc(sizeof(Table));
    table->noRows = numRows;
    table->currId = currentMaxId++;
    table->pager = pager;

    return table;
}

void databaseClose(Table* table) {
    Pager* pager = table->pager;
    uint32_t numFullPages = table->noRows / ROWS_PER_PAGE;

    for (uint32_t i = 0; i < numFullPages; i++) {
        if (pager->pages[i] == NULL) {
        continue;
        }
        pagerFlush(pager, i, PAGE_SIZE);
        free(pager->pages[i]);
        pager->pages[i] = NULL;
    }

    // There may be a partial page to write to the end of the file
    // This should not be needed after we switch to a B-tree
    uint32_t numAdditionalRows = table->noRows % ROWS_PER_PAGE;
    if (numAdditionalRows > 0) {
        uint32_t pageNum = numFullPages;
        if (pager->pages[pageNum] != NULL) {
        pagerFlush(pager, pageNum, numAdditionalRows * ROW_SIZE);
        free(pager->pages[pageNum]);
        pager->pages[pageNum] = NULL;
        }
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

    for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
        pager->pages[i] = NULL;
    }

    return pager;
}

void pagerFlush(Pager* pager, uint32_t pageNum, uint32_t size) {
    if (pager->pages[pageNum] == NULL) {
        printf("Tried to flush null page\n");
        exit(EXIT_FAILURE);
    }

    off_t offset = lseek(pager->fileDescriptor, pageNum * PAGE_SIZE, SEEK_SET);

    if (offset == -1) {
        printf("Error seeking: %d\n", errno);
        exit(EXIT_FAILURE);
    }

    ssize_t bytesWritten =
        write(pager->fileDescriptor, pager->pages[pageNum], size);

    if (bytesWritten == -1) {
        printf("Error writing: %d\n", errno);
        exit(EXIT_FAILURE);
  }
}

void printTable(Table *table) {
    for (uint32_t i = 0; i < table->noRows; i++) {
        Row row;
        deserialiseRow(rowSlot(table, i), &row);
        printRow(&row);
    }
} 

Cursor *tableStart(Table *table) {
    Cursor *cursor = malloc(sizeof(Cursor));
    cursor->table = table;
    cursor->rowNum = 0;
    cursor->endOfTable = (table->noRows == 0);

    return cursor;
}

Cursor *tableEnd(Table *table) {
    Cursor *cursor = malloc(sizeof(Cursor));
    cursor->table = table;
    cursor->rowNum = table->noRows;
    cursor->endOfTable = true;

    return cursor;
}