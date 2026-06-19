########################################################################
### Prerequisites:  sqlite3 dev and pugixml dev
###
### sudo apt install libsqlite3-dev libpugixml-dev
########################################################################

# Variables
CXX = g++
CXXFLAGS = -std=c++20 -O3 -Wall -Wextra
LIBS = -lpugixml -lsqlite3
TARGET = saf
SRC = saf.cpp

# Default target
all: $(TARGET)

# Link and build the executable
$(TARGET): $(SRC)
	@echo "[+] Compiling $(TARGET)..."
	$(CXX) $(CXXFLAGS) $(SRC) $(LIBS) -o $(TARGET)

# Check for system dependencies
check-deps:
	@echo "[*] Checking development headers..."
	@pkg-config --exists pugixml || \
	echo "[-] Warning: pugixml missing"
	@pkg-config --exists sqlite3 || \
	echo "[-] Warning: sqlite3 missing"

# Clean up build artifacts and local db files
clean:
	@echo "[-] Cleaning build artifacts..."
	rm -f $(TARGET)
	rm -f indi_forward_queue.db

# Run the program after compiling
run: $(TARGET)
	./$(TARGET)

.PHONY: all clean run check-deps
