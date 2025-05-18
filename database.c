#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include <stdint.h>
#include <stddef.h>
#include <sys/types.h>

// GLOBAL VARIABLES
#ifdef _WIN32
#include <BaseTsd.h>
typedef SSIZE_T ssize_t;
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
// TYPEDEFS
typedef struct {
    char *input;
    size_t bufferLength;
    size_t length;
} InputBuffer;

typedef struct {
    uint32_t id;
    char *userName;
    char *email;
} Row;

typedef struct {
    uint32_t noRows;
    void *pages[TABLE_MAX_PAGES];
} Table;

// ENUM DEFINITIONS


// Program
void printCommands();
void printPrompt();
InputBuffer *NewInputBuffer();
void readInput(InputBuffer *inputBuffer);
void closeInput(InputBuffer *InputBuffer);
ssize_t getLine(char **linePtr, size_t *n, FILE *stream);
void readAndDoCommand(InputBuffer *inputBuffer, Table *table);
void doInsert(InputBuffer *inputBuffer, Table *table);
void doSelect(InputBuffer *inputBuffer);
bool isNumber(char *number);
bool isUserName(char *userName);
bool isEmail(char *email);
void printRow(Row *row);
void serialiseRow(Row *source, void *destination);
void deserialiseRow(void *source, Row *destination);
void *rowSlot(Table *table, uint32_t rowNum);
Table *NewTable();
void FreeTable();
bool insertArgsCheck(char *arg, int len);

int main(int argc, char *argv[]) {
    Table *table = NewTable(); 
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
    printf("insert: To insert data\n");
    printf("delete: To delete data\n");
    printf("modify: To modify data\n");
    printf("select: To select data\n");
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
        *linePtr = malloc(*n * sizeof(char *));
        if (*linePtr == NULL) return -1;
    }

    while ((c = fgetc(stream)) != EOF) {
        if (pos + 1 >= *n) {
            *n += chunk;
            char *newPtr = realloc(*linePtr, *n * sizeof(char *));
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
        closeInput(inputBuffer);
        exit(EXIT_SUCCESS);
    } else if (strcmp(inputBuffer->input, "help") == 0) {
        printCommands();
    }  else { 
        char *command = strtok(inputBuffer->input, " ");
        if (strcmp(command, "insert") == 0) {
            doInsert(inputBuffer, table);
        } else if (strcmp(command, "select") == 0) {
            doSelect(inputBuffer);
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
    rowToInsert->id = atoi(insert_args[0]);
    rowToInsert->userName = strdup(insert_args[1]);
    rowToInsert->email = strdup(insert_args[2]);
    printRow(rowToInsert);

}

bool insertArgsCheck(char *arg, int len) {
    if (len == 0) {
        if (!isNumber(arg)) {
            printf("invalid id\n");
            return false;
        }
    } else if (len == 1) {
        if (!isUserName(arg)) {
            printf("Invalid username\n");
            return false;
        }
        printf("length = 1 argis %s\n", arg);
    } else {
        if (!isEmail(arg)) {
            printf("Invalid email\n");
            return false;
        }
    }

    return true;
}

void doSelect(InputBuffer *inputBuffer) {
    printf("Input buffer is:\n");
}

bool isNumber(char *number) {
    if (atoi(number) == 0) {
        return false;
    }

    return true;
}

bool isUserName(char *userName) {
    if (strlen(userName) <= 5) {
        return false;
    }

    return true;
}

bool isEmail(char *email) {
    if (strstr(email, "@") == NULL) {
        return false;
    }

    return true;
}

void printRow(Row *row) {
    printf("%d, %s, %s\n", row->id, row->userName, row->email);
}

void serialiseRow(Row *source, void *destination) {
    memcpy((char *)destination + ID_OFFSET, &(source->id), ID_SIZE);
    memcpy((char *)destination + USERNAME_OFFSET, &(source->userName), USERNAME_SIZE);
    memcpy((char *)destination + EMAIL_OFFSET, &(source->email), EMAIL_SIZE);
}

void deserialiseRow(void *source, Row *destination) {
    memcpy(&(destination->id), (char *)source + ID_OFFSET, ID_SIZE);
    memcpy(&(destination->userName), (char *)source + USERNAME_OFFSET, USERNAME_SIZE);
    memcpy(&(destination->email), (char *)source + EMAIL_OFFSET, EMAIL_SIZE);
}

void *rowSlot(Table *table, uint32_t rowNum) {
    uint32_t pageNum = rowNum / ROWS_PER_PAGE;
    void *page = table->pages[pageNum];
    if (page == NULL) {
        page = table->pages[pageNum] = malloc(PAGE_SIZE);
    }

    uint32_t rowOffset = rowNum % ROWS_PER_PAGE;
    uint32_t byteOffset = rowOffset * ROW_SIZE;
    return ((char *)page + byteOffset);
}

Table *NewTable() {
    Table *table = malloc(sizeof(Table));

    table->noRows = 0;
    for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
        table->pages[i] = NULL;
    }

    return table;
}

void FreeTable(Table *table) {
    for (uint32_t i = 0; table->pages[i]; i++) {
        free(table->pages[i]);
    }

    free(table);
}