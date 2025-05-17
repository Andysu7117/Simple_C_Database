#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include <stddef.h>
#include <sys/types.h>

// TYPEDEFS
typedef struct {
    char *input;
    size_t bufferLength;
    size_t length;
} InputBuffer;


// ENUM DEFINITIONS

// GLOBAL VARIABLES
#ifdef _WIN32
#include <BaseTsd.h>
typedef SSIZE_T ssize_t;
#endif

// Program
void printCommands();
void printPrompt();
InputBuffer *NewInputBuffer();
void readInput(InputBuffer *inputBuffer);
void closeInput(InputBuffer *InputBuffer);
ssize_t getLine(char **linePtr, size_t *n, FILE *stream);

int main(int argc, char *argv[]) {
    printCommands();
    InputBuffer *inputBuffer = NewInputBuffer();
    while (true) {
        printPrompt();
        readInput(inputBuffer);
        if (strcmp(inputBuffer->input, "exit") == 0) {
            closeInput(inputBuffer);
            exit(EXIT_SUCCESS);
        } else if (strcmp(inputBuffer->input, "help") == 0) {
            printCommands();
        } else {
            printf("Unrecognised Command %s\n", inputBuffer->input);
        }
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