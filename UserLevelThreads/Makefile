CC = g++
CXX = g++

CODESRC = uthreads.cpp Thread.cpp
HEADERS = uthreads.h Thread.h
OBJECTS = uthreads.o Thread.o

LIBNAME = libuthreads.a

INCS = -I.
CFLAGS = -Wall -std=c++11 -c $(INCS)
CXXFLAGS = -Wall -std=c++11 -c $(INCS)

TAR = tar
TARFLAGS = -cvf
TARNAME = ex2.tar
TARSRCS = $(CODESRC) $(HEADERS) Makefile README

all: $(LIBNAME)

$(LIBNAME): $(OBJECTS)
	ar rcs $@ $^

%.o: %.cpp $(HEADERS)
	$(CXX) $(CXXFLAGS) $<

clean:
	$(RM) $(OBJECTS) $(LIBNAME)

tar:
	$(TAR) $(TARFLAGS) $(TARNAME) $(TARSRCS)
