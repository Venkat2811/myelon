MPICC ?= mpicc
CC := $(MPICC)
UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Darwin)
  CFLAGS := -O3 -std=c11 -flto -DNDEBUG -D_POSIX_C_SOURCE=200809L
  LDFLAGS := -flto
else
  CFLAGS := -O3 -std=c11 -march=native -flto -DNDEBUG -D_POSIX_C_SOURCE=200809L
  LDFLAGS := -flto -lrt
endif

TARGET := pingpong
SRC := pingpong.c

all: $(TARGET)

$(TARGET): $(SRC)
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)

clean:
	rm -f $(TARGET)

.PHONY: all clean
