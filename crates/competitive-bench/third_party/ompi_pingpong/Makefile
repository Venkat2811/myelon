MPICC ?= mpicc
CC := $(MPICC)
CFLAGS := -O3 -std=c11 -march=native -flto -DNDEBUG -D_POSIX_C_SOURCE=200809L
LDFLAGS := -flto -lrt

TARGET := pingpong
SRC := pingpong.c

all: $(TARGET)

$(TARGET): $(SRC)
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)

clean:
	rm -f $(TARGET)

.PHONY: all clean
