CXX := g++
UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Darwin)
  BOOST_PREFIX ?= $(shell brew --prefix boost 2>/dev/null || echo /opt/homebrew)
  CXXFLAGS := -O3 -std=c++17 -pthread -flto -DNDEBUG -I$(BOOST_PREFIX)/include
  LDFLAGS := -flto
else
  CXXFLAGS := -O3 -std=c++17 -pthread -march=native -flto -DNDEBUG
  LDFLAGS := -lrt -flto
endif

TARGET := boost_pingpong
SRC := main.cpp

all: $(TARGET)

$(TARGET): $(SRC)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS)

clean:
	rm -f $(TARGET)

.PHONY: all clean
