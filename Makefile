CC      = gcc
CFLAGS  = -Wall -Wextra -Werror -g -Iinclude
LIBS    = -lrt
BIN_DIR = bin

all: $(BIN_DIR)/msgq_lab

check: all
	./scripts/check.sh

grade: all
	./scripts/grade.sh

$(BIN_DIR)/msgq_lab: src/msgq_lab.c include/msgq_lab.h | $(BIN_DIR)
	$(CC) $(CFLAGS) $< -o $@ $(LIBS)

$(BIN_DIR):
	mkdir -p $(BIN_DIR)

clean:
	rm -f $(BIN_DIR)/msgq_lab

.PHONY: all check grade clean