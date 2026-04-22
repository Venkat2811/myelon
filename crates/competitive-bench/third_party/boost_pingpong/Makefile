CXX := g++
CXXFLAGS := -O3 -std=c++17 -pthread -march=native -flto -DNDEBUG
LDFLAGS := -lrt -flto

TARGET := boost_pingpong
SRC := main.cpp

all: $(TARGET)

$(TARGET): $(SRC)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS)

clean:
	rm -f $(TARGET)

.PHONY: all clean
