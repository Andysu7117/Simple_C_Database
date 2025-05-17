#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
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

int main(int argc, char *argv[]) {
    printCommands();
    InputBuffer *inputBuffer = NewInputBuffer();
    while (true) {
        printPrompt();
        readInput(inputBuffer);
    }

    return 0;
}

void printCommands() {
    printf("Commands are:\n");
    printf("helo: for help\n");
    printf("Insert: To insert data\n");
    printf("Delete: To delete data\n");
    printf("Modify: To modify data\n");
    printf("Select: To select data\n");
    printf("Exit: Exits program\n");
}

void printPrompt() {
    printf("db > ");
}

InputBuffer *NewInputBuffer() {
    InputBuffer *newInputBuffer = malloc(sizeof(InputBuffer));
    newInputBuffer->input = NULL;
    newInputBuffer->bufferLength = 0;
    newInputBuffer->length = 0;

    return newInputBuffer;
}

void readInput(InputBuffer *inputBuffer) {
    ssize_t bytesRead = getline(&(inputBuffer->input), &(inputBuffer->bufferLength), stdin);

    if (bytesRead <= 0) {
        printf("Error Reading Input");
        exit(1);
    }

    // Ignore Trailing NewLine and insert null terminator
    inputBuffer->length = bytesRead - 1;
    inputBuffer->input[bytesRead - 1] = 0;
}