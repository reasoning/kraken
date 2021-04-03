
# To force 32bit builds of all dependent libraries, usually this is required
# ./configure CC="gcc -m32" CXX="G++ -m32"

PROJECT=kraken

REASON=./lib/reason/

#CC=g++
CC=clang

INC=-Isrc -Isrc/$(PROJECT) -I$(REASON)lib/sqlite/include -I$(REASON)lib/zlib/include -I$(REASON)lib/openssl/include -I$(REASON)lib/mysql/include -I$(REASON)src -I$(REASON)lib/postgres/include -I$(REASON)lib/ruby/include -I./lib/tidy/include -I./lib/sphinx/include -I./lib/zeromq/include -I./lib/kyototycoon -I./lib/kyotocabinet -I./lib/zeromq -I./lib/leveldb/include


#LIB=-lm -lpthread -lz -ldl -liconv -lexpat -L$(REASON) 
#LIB=-lz -lrt -lm -lpthread -ldl -L$(REASON)  

#LIB=-luuid -lkyotocabinet -lz -lstdc++ -lrt -lpthread -lm -lc -ldl -L$(REASON)
LIB=-luuid -lkyotocabinet -lkyototycoon -lm -lpthread -lz -ldl -lstdc++ -lssl -lcrypto -lrt -fsanitize=address -L$(REASON)
#LIB=-luuid -lm -lpthread -lz -ldl -lstdc++ -lssl -lcrypto -lrt -fsanitize=address -L$(REASON)

#OBJ=$(REASON)libreason.a $(REASON)lib/sqlite/libsqlite3.a $(REASON)lib/openssl/libssl.a $(REASON)lib/openssl/libcrypto.a $(REASON)lib/mysql/libmysqlclient.a $(REASON)lib/postgres/libpq.a $(REASON)lib/lzo/liblzo2.a ./lib/kyotocabinet/libkyotocabinet.a ./lib/kyototycoon/libkyototycoon.a ./lib/zeromq/libzmq.a
OBJ=$(REASON)libreason.a $(REASON)lib/sqlite/libsqlite3.a $(REASON)lib/mysql/libmysqlclient.a $(REASON)lib/postgres/libpq.a $(REASON)lib/lzo/liblzo2.a ./lib/kyotocabinet/libkyotocabinet.a ./lib/kyototycoon/libkyototycoon.a ./lib/zeromq/libzmq.a
#OBJ=$(REASON)libreason.a $(REASON)lib/sqlite/libsqlite3.a $(REASON)lib/mysql/libmysqlclient.a $(REASON)lib/postgres/libpq.a $(REASON)lib/lzo/liblzo2.a ./lib/zeromq/libzmq.a

#./lib/sphinx/libsphinx.a ./lib/tidy/libtidy.a


#OPTARCH=-arch i386
OPTARCH=-m64
#OPTARCH=-m32
#OPTFLAG=-fpermissive -fno-operator-names -fno-rtti -fno-exceptions
#OPTFLAG=-fpermissive -fno-operator-names 
#OPTFLAG=-fpermissive -fno-operator-names -fno-rtti -fno-exceptions
OPTFLAG=-fpermissive -fno-operator-names


#CFLAGS=$(OPTFLAG) $(OPTARCH) -w -Os 
#CFLAGS=$(OPTFLAG) $(OPTARCH) -w -O3 
#CFLAGS=$(OPTFLAG) $(OPTARCH) -w -O3 -ggdb 
CFLAGS=$(OPTFLAG) $(OPTARCH) --std=c++11 -Wno-narrowing -w -O0 -ggdb -fno-omit-frame-pointer -fsanitize=address -D_DEBUG 
#CFLAGS=$(OPTFLAG) $(OPTARCH) -w -O0 -ggdb -D_DEBUG
#CFLAGS=$(OPTFLAG) $(OPTARCH) -w -O2 -g -D_DEBUG 
#CFLAGS=$(OPTFLAG) $(OPTARCH) -fvtable-thunks -w -g -O0 -D_DEBUG 



SRCDIR=src
OBJDIR=build

SRCFILES=$(shell find $(SRCDIR)/ -name "*.cpp")
INCFILES=$(shell find $(SRCDIR)/ -name "*.d")
SUBFILES=$(wildcard *.d)
OBJFILES=$(patsubst %.cpp,%.o,$(SRCFILES))
DEPFILES=$(patsubst %.cpp,%.d,$(SRCFILES))


default: all

clean:
	rm -rf $(OBJFILES)
	rm -rf $(DEPFILES)

all: $(PROJECT) 

$(PROJECT): $(OBJFILES)	 
	$(CC) $(OBJFILES) $(OPTARCH) $(OBJ) $(LIB) -o $@

#%.o: $(SRCFILES)
#%.o:
#$(OBJFILES): $(SRCFILES)

#$(warning $(OBJFILES))

$(OBJFILES):  
	$(CC) $(INC) $(CFLAGS) -c $< -o $@

dep: $(SRCFILES)
	$(CC) $(INC) -MMD -E $(SRCFILES) > /dev/null

#$(warning $(SRCFILES))

depend: $(DEPFILES)

#$(DEPFILES): $(SRCFILES)
#	$(warning $< $@)
#	$(CC) $(INC) $< -E -MM -MF $(patsubst %.cpp,%.d,$<) > /dev/null

# The -MF option combined with the $@ to specify the rule from $(DEPFILES) allows me to 
# specify the correct output file.  This is the only way ive found to do this, you cant
# use $< as it represents only the first dependency, as in the .cpp file which includes
# all the .h files in a dependency output.  The -MT lets me specify the name of the rule
# target which is created so that it can be matched with whats in $(OBJFILES), again we
# want the full path to remove ambiguity.



$(DEPFILES): 
	$(warning $< $@)
	$(CC) $(INC) $(patsubst %.d,%.cpp,$@) -E -MM -MT $(patsubst %.d,%.o,$@) -MF $@ > /dev/null

#$(warning $(INCFILES))
#$(warning $(SUBFILES))

# Ok, so ive got some magic above outputing a dependency file for every path in $(DEPFILES)
# in the relevant subdirectory, but now i dont think we can include them because include
# seems to be missing the full path or cant look in subdirectories.


include $(INCFILES)
