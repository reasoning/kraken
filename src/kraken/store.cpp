/**
 * @project		Kraken
 * @file		store.cpp
 * @author		Emerson Clarke
 * @copywright	(c) 2002-2021. All Rights Reserved.
 * @date		
 * @version		1.0
 * @description	


 */

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////


#include "reason/reason.h"


#ifdef REASON_PLATFORM_POSIX

#include <kccommon.h>
#include <ktremotedb.h>

using namespace std;
using namespace kyototycoon;

#endif


#include <zmq.hpp>

#include <string>
#include <iostream>


////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////


#include "reason/system/number.h"
#include "reason/system/file.h"
#include "reason/system/folder.h"
#include "reason/system/security/security.h"
#include "reason/system/config.h"
#include "reason/system/stringstream.h"
#include "reason/system/binary.h"
#include "reason/structure/tree.h"
#include "reason/system/encoding/transcoder.h"
#include "reason/system/encoding/encoding.h"
#include "reason/system/storage/archive.h"
#include "reason/structure/list.h"
#include "reason/structure/array.h"
#include "reason/messaging/callback.h"
#include "reason/platform/thread.h"
#include "reason/platform/atomic.h"

using namespace Reason::System;
using namespace Reason::System::Storage;
using namespace Reason::System::Encoding;
using namespace Reason::System::Security;
using namespace Reason::Structure;
using namespace Reason::Messaging;

#include "kraken/store.h"

#ifdef KRAKEN_GOOGLE_SPARSEHASH

#undef HAVE_INTTYPES_H
#include <stdint.h>
#include <google/sparse_hash_map>
//#include <google/sparsehash/sparseconfig.h>
//using ext::hash;

struct eqstr
{
	bool operator()(const char* s1, const char* s2) const
	{
		return (s1 == s2) || (s1 && s2 && strcmp(s1, s2) == 0);
	}
};

#endif

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////



////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

void ZmqClient(void *)
{
	//Thread::Sleep(10);
	//for (int i=0;i<100;++i)
	//	long long rand = Number::Random();
	
	printf("Created thread...\n");
	Thread::Sleep(Number::Random()%100);
	

    void *context = zmq_init (1);

    //  Socket to talk to server
    printf ("Connecting to hello world server…\n");
    void *requester = zmq_socket (context, ZMQ_REQ);
    zmq_connect (requester, "tcp://localhost:5555");

    int request_nbr;
    for (request_nbr = 0; request_nbr != 10; request_nbr++) {
        zmq_msg_t request;
        zmq_msg_init_size (&request, 5);
        memcpy (zmq_msg_data (&request), "Hello", 5);
        printf ("Sending Hello %d…\n", request_nbr);
        zmq_send (requester, &request, 0);
        zmq_msg_close (&request);

        zmq_msg_t reply;
        zmq_msg_init (&reply);
        zmq_recv (requester, &reply, 0);
        printf ("Received World %d\n", request_nbr);
        zmq_msg_close (&reply);
    }


	zmq_close (requester);
    zmq_term (context);	

}


void ZmqServer(void *)
{
	printf ("Starting hello world server…\n");
   	//  Prepare our context and socket
    zmq::context_t context (1);
    zmq::socket_t socket (context, ZMQ_REP);
    socket.bind ("tcp://*:5555");

    while (true) {
        zmq::message_t request;

        //  Wait for next request from client
        socket.recv (&request);

		//Console << "Foo %s %d" << hello << 5;
		
        std::cout << "Received Hello" << std::endl;

        //  Do some 'work'
        //sleep (1);

        //  Send reply back to client
        zmq::message_t reply (5);
        memcpy ((void *) reply.data (), "World", 5);
        socket.send (reply);
    }	
	
}



////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Abstract away the details of the key value store, including iterator model so that it can support anything
// such as stl::deque, google::sparse_hash_map, Map<>, or double Arrayset<> structures.


class KeyValueIterator
{
public:
	
	static String Null;
	
	virtual bool Forward() {return false;}
	virtual bool Reverse() {return false;}
	
	virtual bool Has() {return false;}
	
	virtual bool Move(int offset=1) {return false;}
	virtual bool Move(const String & key) {return false;}
	
	virtual String Key() {return Null;}
	virtual String Value() {return Null;}
};		

String KeyValueIterator::Null = "";

class KeyValueStore
{
public:
	
	virtual KeyValueIterator * Iterate()=0;
	
	virtual KeyValueIterator * Forward()
	{
		KeyValueIterator * it = Iterate();
		it->Forward();
		return it;
	}
	
	virtual KeyValueIterator * Reverse()
	{
		KeyValueIterator * it = Iterate();
		it->Reverse();
		return it;
	}	

	enum Accessor
	{
		ACCESS_SEQUENTIAL	= 1,
		ACCESS_RANDOM		= 2,
	};

	int Access;

	KeyValueStore(int access=ACCESS_SEQUENTIAL):Access(access)
	{
	}

	virtual bool Insert(const String & key, const String & value)=0;
	virtual bool Contains(const String & key)=0;
	virtual bool Select(const String & key, String & value)=0;
	
	// If the interface supports random access, then this method will succeed.
	// Otherwise, its going to return false.
	virtual bool Select(int index, String & key, String & value) {return false;}

	//bool Update(const String & ns, const String & key, const String & value)=0;
	
	// Its not necessary to implement this if there is no delete method on the external interface.
	//bool Remove(const String & ns, const String & key) {};
	
	virtual int Keys() {return 0;}
	virtual int Values() {return 0;}
		
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////


class MapStore;

class MapIterator : public KeyValueIterator
{
public:
	
	
	MapStore * Store;
	Iterand<Mapped<String,String> > It;

	MapIterator(MapStore * store):Store(store) {}

	virtual bool Forward();

	virtual bool Has();
	virtual bool Move();
	virtual bool Move(const String & key);

	virtual String Key();
	virtual String Value();
};


class MapStore: public KeyValueStore
{
public:
	
	typedef Reason::Structure::Map<String,String> Mapping;
	
	// Actually, this is now a map of key to hex representation of the pointer
	// to the node in data.  So 2x faster on lookup :)
		
	// A map of namespaces to a map of key to md5 hash of data
	Mapping KeyMap;
		
	// A map of md5 hash of data to the actual data
	Mapping ValueMap;
	
	int Keys() {return KeyMap.Length();}
	int Values() {return ValueMap.Length();}
		
	KeyValueIterator * Iterate()
	{
		return new MapIterator(this);
	}
	
	bool Insert(const String & key, const String & value)
	{	
		// Create a hash, insert it into a map of hash to data, then insert a pointer
		// to that node into our key map, so its only one lookup.		
		Md5 hash(value.Data,value.Size);
		Iterand< Mapped<String,String> > mapit = ValueMap.Update(hash,value);
		if (mapit)
		{
							
			// Make a 64 bit compatible pointer by taking the address of the node in
			// the iterand.
			String hex;
			hex.Format("%016lX",&mapit);
			KeyMap.Insert(key,hex);		
			return true;		
		}			
		
		return false;
	}	
	
	bool Contains(const String & key)
	{
		return ValueMap.Contains(key) != 0;
	}
	
	bool Select(const String & key, String & value)
	{

		Iterand< Mapped<String,String> > mapit = KeyMap.Select(key);
		if (mapit)
		{
			String hex = mapit().Value();
			Iterand< Mapped<String,String> > mapit = (Item<Mapped<String,String>> *)(void*)hex.Hex();
			if (mapit)
			{
				value = mapit().Value();
				return true;
			}	
		}
		
		return false;
	}

};

bool MapIterator::Forward()
{

	It = Store->KeyMap.Forward();
	return It != 0;
}

bool MapIterator::Has()
{
	return It != 0;
}

bool MapIterator::Move()
{
	if (It)
		return ++It != 0;
	
	return false;	
}

bool MapIterator::Move(const String & key)
{
	// Move to the next key after this one, to support cursors
	int step = 1;
	
	Iterand< Mapped<String,String> > mapit;
	if (!((String&)key).IsEmpty())
	{
		//OutputConsole("Using key \"%s\" as cursor\n",key.Print());

		// Now resolve from the key to the md5 hash and get the data
		mapit = Store->KeyMap.Contains(key);
		if (!mapit)
		{
			OutputConsole("MapIterator::Move - Key not found %s\n",((String&)key).Print());
			return false;
		}
	
		// These methods arent exposed in the api, but we can still use
		// the iterand and begin iteration from here thanks to the fact
		// that its all public :)

		mapit.Option = 1;
		Reason::Structure::Abstract::BinaryTree<Mapped<String,String> >::NodeIteration iteration;				


		// Use the instantly available iteration interface to step through
		// records, the step is ignored if this is the first or last record...
		if (step > 0)
			while (step-- > 0)
				iteration.Move(mapit,1);
		//else
		//if (step < 0)
		//	while (step++ < 0)
		//		--mapit;
	}
	else
	{
		//OutputConsole("No key, using first/last as cursor\n");

		// Get the first or last record...
		if (step > 0)
			mapit = Store->KeyMap.Forward();
		//else
			//mapit = nsit().Value().Reverse();
	}


	if (mapit)
	{
		It = mapit;
		return true;
	}

	
	It = (Item<Mapped<String,String> >*)0;
	return false;
}

String MapIterator::Value()
{
	if (It)
	{
		String hex = It().Value();
		Iterand< Mapped<String,String> > mapit = (Item<Mapped<String,String> > *)(void*)hex.Hex();
		if (mapit)
		{
			return mapit().Value();
		}
	}
	
	return Null;
}

String MapIterator::Key()
{
	if (It)
		return It().Key();
		
	return Null;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////




////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
#ifdef KRAKEN_GOOGLE_SPARSEHASH

struct SparseHasher
{	
	unsigned int operator () (const String & string) const
	{
		return string.Hash();
		//return BinaryLibrary::SuperFastHash::Hash(string.Data,string.Size);
	}
};

struct SparseEquality
{
  	bool operator()(const String & left, const String & right) const
  	{
		return left.Compare(right) == 0;
  	}
};

typedef google::sparse_hash_map<String,String,SparseHasher,SparseEquality> SparseHash;

class SparseStore;

class SparseIterator : public KeyValueIterator
{
public:
	
	SparseStore * Store;
	SparseHash::iterator It;
	
	SparseIterator(SparseStore * store):Store(store) {}

	virtual bool Forward();

	virtual bool Has();
	virtual bool Move();
	virtual bool Move(const String & key);

	virtual String Key();
	virtual String Value();
};


class SparseStore: public KeyValueStore
{
public:
	
	// Same basic model as the map store, we split the keys and values to avoid 
	// duplicates.
	SparseHash KeyHash;
	SparseHash ValueHash;

	int Keys() {return KeyHash.size();}
	int Values() {return ValueHash.size();}
		
	KeyValueIterator * Iterate()
	{
		return new SparseIterator(this);
	}
	
	bool Insert(const String & key, const String & value)
	{				
		// Create a hash, insert it into a map of hash to data, then insert a pointer
		// to that node into our key map, so its only one lookup.		
		Md5 hash(value.Data,value.Size);

		ValueHash[hash] = value;
		KeyHash[key] = hash;
		return true;		
	}	
	
	bool Contains(const String & key)
	{
		return KeyHash.find(key) != KeyHash.end();
	}
	
	bool Select(const String & key, String & value)
	{
		SparseHash::iterator it = KeyHash.find(key);
		if (it != KeyHash.end())
		{
			String hash = (*it).second;
			it = ValueHash.find(hash);
			if (it != ValueHash.end())
			{
				value = (*it).second;
				return true;
			}			
		}
		
		return false;
	}

};

bool SparseIterator::Forward()
{
	It = Store->KeyHash.begin();
	return It != Store->KeyHash.end();
}

bool SparseIterator::Has()
{
	return It != Store->KeyHash.end();
}

bool SparseIterator::Move()
{
	if (It != Store->KeyHash.end())
	{
		++It;
		return It != Store->KeyHash.end();
	}
	
	return false;	
}

bool SparseIterator::Move(const String & key)
{
	It = Store->KeyHash.find(key);
	It++;
	return It != Store->KeyHash.end();
}

String SparseIterator::Value()
{
	SparseHash::iterator it = It;
	if (it != Store->KeyHash.end())
	{
		String hash = (*it).second;
		it = Store->ValueHash.find(hash);
		if (it != Store->ValueHash.end())
			return (*it).second;
	}
	
	return Null;
}

String SparseIterator::Key()
{
	SparseHash::iterator it = It;
	if (it != Store->KeyHash.end())
		return (String&)(*it).first;
		
	return Null;
}

#endif
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////




////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////



// A small mapped structure designed to have minimum size, no vtable
// and no inheritance.
class ArrayMapped : public Object
{
public:

	// Key must be ascii null terminated.
	char * Key;

	// Value is binary
	char * Value;
	int Size;

	ArrayMapped():Key(0),Value(0),Size(0)
	{
	}

	ArrayMapped(char * key):Key(key),Value(0),Size(0)
	{
	}

	ArrayMapped(char * key, char * value, int size):Key(key),Value(value),Size(size)
	{
	}

	~ArrayMapped()
	{
		// Assume the strings are created using [] notation, which they are here since
		// we just steal memory from strings.  If we dont use the propper delete we will
		// make a mess of the heap and crash.
		if (Key) delete [] Key;
		if (Value) delete [] Value;
	}

	int Compare(Object * obj,int comparitor)
	{
		ArrayMapped * mapped = (ArrayMapped*)obj;
		return Sequences::Compare(Key,String::Length(Key),mapped->Key,String::Length(mapped->Key));
	}
};

class ArrayStore;

class ArrayIterator : public KeyValueIterator
{
public:


	ArrayStore * Store;
	Iterand<ArrayMapped *> It;
	
	ArrayIterator(ArrayStore * store):Store(store) {}

	virtual bool Forward();

	virtual bool Has();
	virtual bool Move();
	virtual bool Move(const String & key);

	virtual String Key();
	virtual String Value();
};


class ArrayStore: public KeyValueStore
{
public:
	
	// Same basic model as the map store, we split the keys and values to avoid 
	// duplicates.  But importantly we store a pointer to a simple object, that 
	// way the array can use memcpy() and the object size is reduced.
	Arrayset<ArrayMapped *> KeyValue;

	int Keys() {return KeyValue.Length();}
	int Values() {return KeyValue.Length();}
	
	ArrayStore()
	{
		// Reserve 1 million entries!
		//KeyValue.Reserve(1000000);
	}
		
	~ArrayStore()
	{
		// There are a lot of Mapped<> structures that need cleaning up :)
		KeyValue.Destroy();
	}

	ArrayIterator * Iterate()
	{
		return new ArrayIterator(this);
	}
	
	bool Insert(const String & key, const String & value)
	{	
		bool binary = true;
		
		if (KeyValue.Length() > 0)
		{
			// Try optimisitic insertion
			ArrayMapped * m = KeyValue[KeyValue.Length()-1];
			if (((String &)key).Compare(m->Key) > 0)
			{
				KeyValue.Insert(new ArrayMapped(key.Data,value.Data,value.Size),KeyValue.Length());
				binary = false;
			}
		}
		
		if (binary)
		{	
			// Insert using binary search
			KeyValue.Insert(new ArrayMapped(key.Data,value.Data,value.Size));
		}

		// Steal the memory of the key and value
		((String &)key).Data = 0;
		((String &)key).Allocated = ((String &)key).Size = 0;
		((String &)value).Data = 0;
		((String &)value).Allocated = ((String &)value).Size = 0;
		
		// Make the array expand quicker... its hard enough that it does so much copying.
		// Atleast until we have 5M records.
		if (KeyValue.Allocated < KeyValue.Size*2 && KeyValue.Length() < 5000000)
			KeyValue.Reserve(KeyValue.Size*2);
		
		return true;		
	}	
	
	bool Contains(const String & key)
	{
		String mk = key;
		ArrayMapped m(mk.Data);
		mk.Data = 0;
		mk.Allocated = mk.Size = 0;
		return KeyValue.Select(&m);
	}
	
	bool Select(const String & key, String & value)
	{
		String mk = key;
		ArrayMapped m(mk.Data);
		mk.Data = 0;
		mk.Allocated = mk.Size = 0;
		Iterand<ArrayMapped *> it = KeyValue.Select(&m);
		if (it)
		{	
			value.Construct(it()->Value,it()->Size);
			return true;
		}
		
		
		return false;
	}

};

bool ArrayIterator::Forward()
{
	It = Store->KeyValue.Forward();
	return It != 0;
}

bool ArrayIterator::Has()
{
	return It != 0;
}

bool ArrayIterator::Move()
{
	if (It != 0)
		return ++It != 0;
	
	return false;	
}

bool ArrayIterator::Move(const String & key)
{
	// FIXME
	// This is a horribly slow non-internally optimised solution, it should just use
	// the index.  But then, an array based Iterator should know this, so it would be
	// the same hack as the MapIterator uses.
	
	String mk = key;
	ArrayMapped m(mk.Data);
	mk.Data = 0;
	mk.Allocated = mk.Size = 0;
	It = Store->KeyValue.Select(&m);
	It++;
	return It != 0;
}

String ArrayIterator::Value()
{
	if (It)
		return Superstring(It()->Value,It()->Size);

	return Null;
}

String ArrayIterator::Key()
{
	if (It)
		return It()->Key;
		
	return Null;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////




////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////



// A small mapped structure designed to have minimum size, no vtable
// and no inheritance.
class DoubleArrayMapped : public Object
{
public:

	char * Key;
	char * Value;

	DoubleArrayMapped():Key(0),Value(0)
	{
	}

	DoubleArrayMapped(char * key):Key(key),Value(0)
	{
	}

	DoubleArrayMapped(char * key, char * value):Key(key),Value(value)
	{
	}

	~DoubleArrayMapped()
	{
		// Assume the strings are created using [] notation, which they are here since
		// we just steal memory from strings.  If we dont use the propper delete we will
		// make a mess of the heap and crash.
		if (Key) delete [] Key;
		if (Value) delete [] Value;
	}

	int Compare(Object * obj,int comparitor)
	{
		DoubleArrayMapped * mapped = (DoubleArrayMapped*)obj;
		return Sequences::Compare(Key,String::Length(Key),mapped->Key,String::Length(mapped->Key));
	}
};

class DoubleArrayStore;

class DoubleArrayIterator : public KeyValueIterator
{
public:


	DoubleArrayStore * Store;
	Iterand<DoubleArrayMapped *> It;
	
	DoubleArrayIterator(DoubleArrayStore * store):Store(store) {}

	virtual bool Forward();

	virtual bool Has();
	virtual bool Move();
	virtual bool Move(const String & key);

	virtual String Key();
	virtual String Value();
};


class DoubleArrayStore: public KeyValueStore
{
public:
	
	// Same basic model as the map store, we split the keys and values to avoid 
	// duplicates.  But importantly we store a pointer to a simple object, that 
	// way the array can use memcpy() and the object size is reduced.
	Arrayset<DoubleArrayMapped*> KeySet;
	Arrayset<DoubleArrayMapped*> ValueSet;

	int Keys() {return KeySet.Length();}
	int Values() {return ValueSet.Length();}
	
	DoubleArrayStore()
	{
		// Reserve 1 million entries!
		//KeyValue.Reserve(1000000);
	}
		
	~DoubleArrayStore()
	{
		// There are a lot of Mapped<> structures that need cleaning up :)
		KeySet.Destroy();
		ValueSet.Destroy();
	}

	DoubleArrayIterator * Iterate()
	{
		return new DoubleArrayIterator(this);
	}
	
	bool Insert(const String & key, const String & value)
	{	
		Iterand<DoubleArrayMapped *> k;
		Iterand<DoubleArrayMapped *> v;

		Md5 hash(value.Data,value.Size);
		if (ValueSet.Length() > 0)
		{
			DoubleArrayMapped * m = ValueSet[ValueSet.Length()-1];
			if (((String &)key).Compare(m->Key) > 0)
			{
				v = ValueSet.Insert(new DoubleArrayMapped(hash.Data,value.Data));
			}
		}

		if (!v)
			v = ValueSet.Insert(new DoubleArrayMapped(hash.Data,value.Data));

		if (v)
		{
			// Make a 64 bit compatible pointer by taking the address of the node in
			// the iterand.
			String hex;
			hex.Format("%016lX",&v);
			KeySet.Insert(new DoubleArrayMapped(key.Data,hex.Data));	
		}

		// Steal the memory of the key and value
		((String &)key).Data = 0;
		((String &)key).Allocated = ((String &)key).Size = 0;
		((String &)value).Data = 0;
		((String &)value).Allocated = ((String &)value).Size = 0;
		

		// Make the array expand quicker... its hard enough that it does so much copying.
		// Atleast until we have 5M records.
		
		//if (Keys.Allocated < Keys.Size*2 && Keys.Length() < 5000000)
		//	Keys.Reserve(Keys.Size*2);
		
		return true;		
	}	
	
	bool Contains(const String & key)
	{
		String mk = key;
		DoubleArrayMapped m(mk.Data);
		mk.Data = 0;
		mk.Allocated = mk.Size = 0;
		return KeySet.Select(&m);
	}
	
	bool Select(const String & key, String & value)
	{
		String mk = key;
		DoubleArrayMapped m(mk.Data);
		mk.Data = 0;
		mk.Allocated = mk.Size = 0;
		Iterand<DoubleArrayMapped *> it = KeySet.Select(&m);
		if (it)
		{
			String hex = it()->Value;
			DoubleArrayMapped * val = (DoubleArrayMapped*)(void*)hex.Hex();
			value = val->Value;
			return true;
		}
				
		return false;
	}

};

bool DoubleArrayIterator::Forward()
{
	It = Store->KeySet.Forward();
	return It != 0;
}

bool DoubleArrayIterator::Has()
{
	return It != 0;
}

bool DoubleArrayIterator::Move()
{
	if (It != 0)
		return ++It != 0;
	
	return false;	
}

bool DoubleArrayIterator::Move(const String & key)
{
	// FIXME
	// This is a horribly slow non-internally optimised solution, it should just use
	// the index.  But then, an array based Iterator should know this, so it would be
	// the same hack as the MapIterator uses.
	
	String mk = key;
	DoubleArrayMapped m(mk.Data);
	mk.Data = 0;
	mk.Allocated = mk.Size = 0;
	It = Store->KeySet.Select(&m);
	It++;
	return It != 0;
}

String DoubleArrayIterator::Value()
{
	if (It)
		return It()->Value;

	return Null;
}

String DoubleArrayIterator::Key()
{
	if (It)
	{
		String hex = It()->Value;
		DoubleArrayMapped * val = (DoubleArrayMapped*)(void*)hex.Hex();
		return val->Value;
	}
		
	return Null;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////



////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////



template<typename _Kind_>
class OffsetIndex : public Arrayset<_Kind_>
{
public:
	
	Reason::Messaging::Callback<int,const _Kind_ &,const _Kind_ &,int> CompareOffsets;
	Reason::Messaging::Callback<int,const _Kind_ &,const String &,int> CompareOffset;

	OffsetIndex()
	{
		//CompareOffsets = Callback<int,const _Kind_ &,const _Kind_ &,int>(Comparer<_Kind_>::Compare);
		//CompareOffset = Callback<int,const _Kind_ &,const String &,int>(Comparer<_Kind_>::Compare);
	}

	Iterand<_Kind_> Insert(typename Template<_Kind_>::ConstantReference type)
	{
		if (this->Initial->Base->Order.Option == Orderable::ORDER_DEFAULT)
		{
			return Arrayset<_Kind_>::Insert(type);
		}
		else
		{

			int first		=0;
			int last		=this->Size-1; 
			int direction	=0;
			int pivot = 0;
			
			while (first <= last)
			{
				pivot = (int)(((long long)first+(long long)last)>>1);
				if (pivot < 0 || pivot >= this->Size) break;

				direction = this->CompareOffsets(this->Data[pivot],type,this->Initial->Base->Compare.Option);

				switch (this->Initial->Base->Order.Option)
				{
				case Orderable::ORDER_ASCENDING:direction=(direction>0)?-1:1;break;
				case Orderable::ORDER_DESCENDING:direction=(direction>0)?1:-1;break;
				}

				switch(direction)
				{
				case  1:
					first = pivot+1;
					if (first > last)
						++pivot;
					break;
				case -1:
					last  = pivot-1;
					break;
				}

			}
				
			
			if (pivot < 0 || pivot >= this->Size)
				return Arrayset<_Kind_>::Insert(type,this->Size);
			else
				return Arrayset<_Kind_>::Insert(type,pivot);
		}
	}

	using Arrayset<_Kind_>::Select;
	Iterand<_Kind_> Select(typename Template<_Kind_>::ConstantReference type, int comparitor = Reason::System::Comparable::COMPARE_GENERAL)
	{
		if (this->Initial->Base->Order.Option == Orderable::ORDER_DEFAULT || comparitor != this->Initial->Base->Compare.Option)
		{
			return Arrayset<_Kind_>::Select(type,comparitor);
		}
		else
		{
			int first		=0;
			int last		=this->Size-1; 
			int direction	=0;

			int pivot = 0;

			while (first <= last)
			{
				pivot = (int)(((long long)first+(long long)last)>>1);
				if (pivot < 0 || pivot >= this->Size) break;

				if ((direction = this->CompareOffsets(this->Data[pivot],type,this->Initial->Base->Compare.Option)) == 0)
				{
					return Iterand<_Kind_>(&this->Data[pivot]);
				}

				switch (this->Initial->Base->Order.Option)
				{
				case Orderable::ORDER_ASCENDING:direction=(direction>0)?-1:1;break;
				case Orderable::ORDER_DESCENDING:direction=(direction>0)?1:-1;break;
				}

				switch(direction)
				{
				case  1:first = pivot+1;break;
				case -1:last  = pivot-1;break;
				}

			}

		}

		return Iterand<_Kind_>();
	}

	Iterand<_Kind_> Select(const String & key, int comparitor = Reason::System::Comparable::COMPARE_GENERAL)
	{
		if (this->Initial->Base->Order.Option == Orderable::ORDER_DEFAULT || comparitor != this->Initial->Base->Compare.Option)
		{
			//OutputConsole("OffsetIndex::Select - Error, key select will not work on non sorted index.\n");
			return Iterand<_Kind_>();
		}
		else
		{
			int first		=0;
			int last		=this->Size-1; 
			int direction	=0;

			int pivot = 0;

			while (first <= last)
			{
				pivot = (int)(((long long)first+(long long)last)>>1);
				if (pivot < 0 || pivot >= this->Size) break;

				if ((direction = this->CompareOffset(this->Data[pivot],key,this->Initial->Base->Compare.Option)) == 0)
				{
					return Iterand<_Kind_>(&this->Data[pivot]);
				}

				switch (this->Initial->Base->Order.Option)
				{
				case Orderable::ORDER_ASCENDING:direction=(direction>0)?-1:1;break;
				case Orderable::ORDER_DESCENDING:direction=(direction>0)?1:-1;break;
				}

				switch(direction)
				{
				case  1:first = pivot+1;break;
				case -1:last  = pivot-1;break;
				}

			}

		}

		return Iterand<_Kind_>();
	}

	struct SortSync
	{
		// Block on a condition until there are no more sort threads running.
		int Count;
		Reason::Platform::Critical Critical;
		Reason::Platform::Condition Condition;
		
		SortSync():Count(0)
		{
		}

		void Inc()
		{
			Atomic::Inc(&Count);
		}

		void Dec()
		{
			if (Count > 0 && Atomic::Dec(&Count) == 0)
				Condition.Signal();
		}

		bool Wait()
		{
			Condition.Wait(Critical);
			return true;
		}
	};

	struct SortData
	{
		int Left;
		int Right;
		int Parallel;
		SortSync & Sync;

		SortData(SortSync & sync, int left=0,int right=0, int parallel=0):Sync(sync),Left(left),Right(right),Parallel(parallel) {}
	};

	using Arrayset<_Kind_>::Swap;

	using Arrayset<_Kind_>::Sort;
	void Sort(int first, int last)
	{
		SortSync sync;
		// Override the virtual function and redirect to the parallel sort :)
		Sort(sync,first,last,2);

		sync.Wait();

		OutputConsole("OffsetIndex::Sort - Sorting completed !\n");
	}

	void Sort(SortSync & sync, int first, int last,int parallel)
	{
		while (first < last)
		{
			int right = last;
			int left = first;

			int pivot = (int)(((long long)first+(long long)last)>>1);
			if (pivot < 0 || pivot >= last) break;

			while(right >= left)
			{			
				while (left <= right && this->CompareOffsets(this->Data[left],this->Data[pivot],this->Initial->Base->Compare.Option) <= 0)
					++left;

				while (left <= right && this->CompareOffsets(this->Data[right],this->Data[pivot],this->Initial->Base->Compare.Option) > 0)
					--right;

				if (left > right) break;
				Swap(left,right);

				if (pivot == right) pivot = left;

				left++; 
				right--;
			}
			
			Swap(pivot,right);
			right--;

			if (parallel > 0)
			{
				// Parallel sort using threads. Since we are trying to make this efficient, do the largest chunk 
				// on another thread, and the smallest in this one.  That way we reduce recursion...
				if (abs(right-first) > abs(last-left))
				{
			
					if (first < right) Fibre::Start(Callback<void,void*>(this,&OffsetIndex::SortParallel),new SortData(sync,first,right,parallel-1));
					first = left;
				}
				else
				{
					if (left < last) Fibre::Start(Callback<void,void*>(this,&OffsetIndex::SortParallel),new SortData(sync,left,last,parallel-1));
					last = right;
				}
			}
			else
			{
				// Use recursion to sort the smallest partition, this increases performance.
				if (abs(right-first) > abs(last-left))
				{
					if (left < last) Sort(sync,left,last,-1);	
					last = right;
				}
				else
				{
					if (first < right) Sort(sync,first,right,-1);
					first = left;
				}
			}
		}
	}

	void SortParallel(void * pointer)
	{
		Fibre * fibre = (Fibre*) pointer;
		SortData * data = (SortData*)fibre->Context;
		data->Sync.Inc();
		OutputConsole("OffsetIndex::SortParallel - Starting parallel sort on thread %08lX with data(%d,%d)\n",fibre->Thread.Id,data->Left,data->Right);
		Sort(data->Sync,data->Left,data->Right,data->Parallel);
		OutputConsole("OffsetIndex::SortParallel - Finished parallel sort on thread %08lX\n",fibre->Thread.Id);
		data->Sync.Dec();
		delete data;
	}

};



struct OffsetMapped
{
	// 32 bit key offset and 32 bit value offset
	// We split the storage into 2GB memory blocks, and index them using the offset map.
	// We can index 50m entries with 400MB.
	// We store the key and the value in separate blocks.
	int Key;
	int Value;

	OffsetMapped():Key(0),Value(0)
	{
	}

	OffsetMapped(int key, int value=0):Key(key),Value(value)
	{
	}

	~OffsetMapped()
	{
	}

};


struct OffsetContainer
{
	// This structure contains all the information for a single massive chunk
	// of keys and values, stored as an index into a binary block.
	OffsetIndex<OffsetMapped> Index;
	StringStream Keys;
	StringStream Values;

	int CompareOffsets(const OffsetMapped & left,const OffsetMapped & right, int comparitor)
	{
		// So the offsets are 32 bit int's that represent offsets within this container.  The
		// offsets are stored as non pointers so that as the container grows they stay valid.
		Substring ls;
		ls.Size = *(int*)(char*)(Keys.Data+left.Key);
		ls.Data = (char*)(Keys.Data+left.Key+4);

		Substring rs;
		rs.Size = *(int*)(char*)(Keys.Data+right.Key);
		rs.Data = (char*)(Keys.Data+right.Key+4);

		return ls.Compare(rs);
	}

	int CompareOffset(const OffsetMapped & left,const String & right, int comparitor)
	{
		// Compare an offset mapped, to a string... for ordered insertion and selection where
		// the rhs is not in the index.
		Substring ls;
		ls.Size = *(int*)(char*)(Keys.Data+left.Key);
		ls.Data = (char*)(Keys.Data+left.Key+4);

		return ls.Compare(right);
	}
};

class OffsetStore;

class OffsetIterator : public KeyValueIterator
{
public:


	OffsetStore * Store;
	int Container;
	int Index;

	
	OffsetIterator(OffsetStore * store,int container=0,int index=0):Store(store),Container(container),Index(index) {}

	virtual bool Forward();

	virtual bool Has();
	virtual bool Move(int offset);
	virtual bool Move(const String & key);

	virtual String Key();
	virtual String Value();
};


class OffsetStore: public KeyValueStore
{
public:
	

	Array<OffsetContainer * > Offsets;

	// Maximum offset is 4GB/2
	// 4GB = 0xFFFFFFFF (unsigned)
	// 2GB = 0x7FFFFFFF (signed)
	static const int MaxOffset = 0x7FFFFFFF>>1;
	
	// But stay 4096 bytes away from danger !!
	static const int SafeOffset = 4096;
	
	OffsetStore():KeyValueStore(KeyValueStore::ACCESS_RANDOM|KeyValueStore::ACCESS_SEQUENTIAL)
	{

	}
		
	~OffsetStore()
	{
		// Delete all the pointers to containers...  that's it, all cleaned up!
		Offsets.Destroy();
	}


	int Keys() 
	{
		int keys=0;
		for (int i=0;i<Offsets.Size;++i)
			keys += Offsets.Data[i]->Index.Size;
		return keys;
	}

	int Values() 
	{
		// There is one value for every key
		return Keys();
	}


	void Offset()
	{
		OffsetContainer * offsets = new OffsetContainer();

		// Make the compare functions for the index use the functions on the container.
		// Could also pass the index a pointer to the container, whatever works.
		offsets->Index.CompareOffsets = Callback<int,const OffsetMapped &,const OffsetMapped &,int>(offsets,&OffsetContainer::CompareOffsets);
		offsets->Index.CompareOffset = Callback<int,const OffsetMapped &,const String &,int>(offsets,&OffsetContainer::CompareOffset);
		
		offsets->Index.Initial->Base->Order.Option = Orderable::ORDER_DEFAULT;
		Offsets.Append(offsets);
	}

	void Sort(bool sorted=false, bool verify=false)
	{
		// Passing sort with sorted as true will just set the ascending order, this is used when
		// we load pre-sorted data but dont want worst case quicksort performance.

		// Without sort, this will never work... since we disabled ordered lookup during insertion
		// when creating each container.
		for (int i=0;i<Offsets.Size;++i)
		{
			Offsets[i]->Index.Initial->Base->Order.Option = Orderable::ORDER_ASCENDING;
			if (!sorted)
			{	
				OutputConsole("OffsetStore::Sort - Sorting unordered data !\n");
				Offsets[i]->Index.Sort();
			}
			else
			{
				OutputConsole("OffsetStore::Sort - Not sorting ordered data !\n");
			}

			// If verify, then ensure that the sort worked and every row is less than the following
			// row in the data index...
			if (verify)
			{
				
				OutputConsole("OffsetStore::Sort - Verifying sort !\n");

				for (int o=0;o<Offsets[i]->Index.Size-1;++o)
				{
					int compare = Offsets[i]->CompareOffsets(Offsets[i]->Index[o],Offsets[i]->Index[o+1],0);
					if (compare > 0)
					{
						OutputAssert(compare <= 0);
						OutputConsole("OffsetStore::Sort - Invalid comparison at index: (%d,%d)\n",o,o+1);
					}
				}
			}
		}
	}

	OffsetIterator * Iterate()
	{
		return new OffsetIterator(this);
	}
	
	bool Insert(const String & key, const String & value)
	{	
		// We can never do a sorted insertion, only append...
		// Sorting can only take place when the offset mapped types are in the same
		// offset container.

		if (Offsets.Length() == 0)
		{
			Offset();
		}

		OffsetContainer * offsets = 0;
		for (int i=0;i<Offsets.Size;++i)
		{
			// Be damn sure that neither the string stream or the array pass over
			// the 4GB barrier, or all hell will break loose !!
			if (Offsets.Data[i]->Index.Size+SafeOffset < MaxOffset &&
				Offsets.Data[i]->Keys.Size+key.Size+SafeOffset < MaxOffset &&
				Offsets.Data[i]->Values.Size+value.Size+SafeOffset < MaxOffset)
			{
				offsets = Offsets[i];
				break;
			}
		}

		if (!offsets)
		{
			Offset();
			offsets = Offsets[Offsets.Size-1];
		}


		if (offsets)
		{			
			int ko = offsets->Keys.Size;
			int vo = offsets->Values.Size;

			offsets->Keys.Append((char*)&key.Size,4);
			offsets->Keys.Append(key.Data,key.Size);
			offsets->Values.Append((char*)&value.Size,4);
			offsets->Values.Append(value.Data,value.Size);

			offsets->Index.Insert(OffsetMapped((int)ko,(int)vo));
		}

		return true;		
	}	
	
	bool Contains(const String & key)
	{
		OffsetContainer * offsets = 0;
		for (int i=0;i<Offsets.Size;++i)
		{
			offsets = Offsets.Data[i];

			Iterand<OffsetMapped> it = offsets->Index.Select(key);
			if (it)
				return true;
		}

		return false;
	}
	
	bool Select(const String & key, String & value)
	{
		OffsetContainer * offsets = 0;
		for (int i=0;i<Offsets.Size;++i)
		{
			offsets = Offsets.Data[i];

			Iterand<OffsetMapped> it = offsets->Index.Select(key);
			if (it)
			{
				// Make a substring which represents the value (no copy)
				Substring v;
				v.Size = *(int*)(char*)(offsets->Values.Data+it().Value);
				v.Data = (char*)(offsets->Values.Data+it().Value+4);

				// And copy it into the value to return
				value = v;
				return true;
			}
		}

		return false;
	}

	bool Select(int index, String & key, String & value)
	{
		if (index < 0)
			return false;

		OffsetContainer * offsets = 0;
		for (int i=0;i<Offsets.Size;++i)
		{
			offsets = Offsets.Data[i];
			if (index < offsets->Index.Size)
			{
				OffsetMapped mapped = offsets->Index.Data[index];
				// Make a substring which represents the value (no copy)
				Substring k;
				k.Size = *(int*)(char*)(offsets->Keys.Data+mapped.Key);
				k.Data = (char*)(offsets->Keys.Data+mapped.Key+4);
				key = k;

				Substring v;
				v.Size = *(int*)(char*)(offsets->Values.Data+mapped.Value);
				v.Data = (char*)(offsets->Values.Data+mapped.Value+4);
				value = v;

				return true;
			}
			else
			{
				index -= offsets->Index.Size;
			}
		}

		return false;

	}

};


bool OffsetIterator::Forward()
{
	Container = Index = 0;
	return true;
}

bool OffsetIterator::Has()
{
	return Container < Store->Offsets.Size && Index < Store->Offsets.Data[Container]->Index.Size;
}

bool OffsetIterator::Move(int offset)
{
	while (offset > 0 && Container < Store->Offsets.Size)
	{
		if (Container < Store->Offsets.Size && Index < Store->Offsets.Data[Container]->Index.Size)
		{
			int size = Store->Offsets.Data[Container]->Index.Size - Index;
			if (offset > size)
			{
				Index += size;
				offset -= size;
				++Container;
			}
			else
			{
				Index += offset;
				offset = 0;
			}
		}
		else
		{
			// Allow moving 1 past the end, just once to signify nothing more to iterate
			Container++;
		
		}

		--offset;
	}
	
	return Has();
}

bool OffsetIterator::Move(const String & key)
{
	// Move next after this key... means we have to select the key index, and then increment.
	// This is a bit hacky, but since the key and value are physically part of the index array
	// we can just take their address and subtract the address of the array.

	OffsetContainer * offsets = 0;
	int container = 0;
	for (int container=0;container<Store->Offsets.Size;++container)
	{
		offsets = Store->Offsets.Data[container];

		Iterand<OffsetMapped> it = offsets->Index.Select(key);
		if (it)
		{
			// Make a substring which represents the value (no copy)

			int index = (int)((OffsetMapped*)&(it().Key) - offsets->Index.Data);
			if (index+1 < offsets->Index.Size)
			{
				Container = container;
				Index = index+1;
				return true;
			}
			else
			{
				Container++;
				Index = 0;
				return Has();
			}
		}
	}

	return false;
}

String OffsetIterator::Value()
{
	if (Has())
	{
		OffsetContainer * offsets = Store->Offsets.Data[Container];
		OffsetMapped mapped = offsets->Index[Index];
		Substring v;
		v.Size = *(int*)(char*)(offsets->Values.Data+mapped.Value);
		v.Data = (char*)(offsets->Values.Data+mapped.Value+4);
		return v;
	}

	return Null;
}

String OffsetIterator::Key()
{
	if (Has())
	{
		OffsetContainer * offsets = Store->Offsets.Data[Container];
		OffsetMapped mapped = offsets->Index[Index];
		Substring k;
		k.Size = *(int*)(char*)(offsets->Keys.Data+mapped.Key);
		k.Data = (char*)(offsets->Keys.Data+mapped.Key+4);
		return k;
	}
		
	return Null;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////


////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

class Server : public Threaded
{
public:

	//Reason::Structure::Map<String,KeyValueStore> Namespaces;
	
	//typedef SparseStore Store;
	//typedef DoubleArrayStore Store;
	
	// The define is to disable ordering during insertion, and sort once its all 
	// loaded into memory.	
	//typedef ArrayStore Store;
	//#define KRAKEN_USING_ARRAYSTORE
	
	typedef OffsetStore Store;
	#define KRAKEN_USING_OFFSETSTORE

	//typedef MapStore Store;
	
	Reason::Structure::Map<String,Store> Namespaces;


	enum Method
	{
		METHOD_GET = 2,
		METHOD_PUT = 4,
		METHOD_CUR = 8,		// Cursor through all keys/values
		METHOD_RND = 16,	// Return random key/value
		METHOD_IDX = 32,	// Return indexed key/value		
	};
	
	enum Status
	{
		STATUS_ERROR	= 0,
		STATUS_OK		= 1,
		STATUS_EOF		= 2,	// End of cursor or similar
	};
	
	struct Request 
	{
		// Data can contain one or more keys, the request is read
		// until there are no more keys to read.  If the method is
		// not get, for example cursor... then the parameters to
		// the method are in data in sequential order.
		int Method;
		String Namespace;
		String Data[0];
	};
	
	struct Response
	{
		// Response is a status followed by zero or more messages.
		// If response is error, then the messaging is the 
		// error code.
		// If response is ok, then the messages follow one after the 
		// other.
		int Status;
		String Data[0];		
	};
	
    zmq::context_t Context;	
	String Port;
	String Path;
	bool Verify;

	Server(const String & port="5555",const String & path="data"):Context(1),Port(port),Path(path),Verify(false)
	{
	   	//  Prepare our context and socket
	    //zmq::context_t context (1);
	    //zmq::socket_t socket (context, ZMQ_REP);
	    //socket.bind ("tcp://*:5555");	
	}
	
	static bool ReadRequest(const String & request, int & method, String & ns, Array<String> & data)
	{
		StringStream::StreamAdapter str((String&)request);
		BinaryStream bin(str);
		
		// No error checking so dont fuck it up...
		int size=0;
		bin.Read(method);			
		bin.Read(size);
		bin.Read(ns,size);
		
		String string;
		while(bin.Read(size) > 0)
		{
			bin.Read(string,size);
			data.Append(string);
			string.Release();
		}
		
		/*
		OutputConsole("ReadRequest - Method: %d, Namespace: %s\n",method,ns.Print());
		for(Iterand<String> it = data.Forward();it != 0;++it)
		{
			OutputConsole("Data: (%d) %s\n",it().Size,it().Print());							
		}
		OutputConsole("\n\n");	
		*/
		

		return true;
	}

	static bool WriteRequest(String & request, int method, const String & ns, Array<String> & data)
	{
		StringStream::StreamAdapter str((String&)request);
		BinaryStream bin(str);	
		bin.Write(method);
		bin.Write(ns.Size);
		bin.Write((String&)ns);
		
		Iterand<String> it = data.Forward();
		while(it)
		{
			bin.Write(it().Size);
			bin.Write(it());	
			++it;				
		}		
		
		return true;		
	}

	static bool ReadResponse(const String & response, int & status, Array<String> & data)
	{
		StringStream::StreamAdapter str((String&)response);
		BinaryStream bin(str);		
		
		int size=0;
		bin.Read(status);
		String string;
		while(bin.Read(size) > 0)
		{
			bin.Read(string,size);
			data.Append(string);
			string.Release();
		}
		
		return true;		
	}

	static bool WriteResponse(String & response, int status, Array<String> & data)
	{
		StringStream::StreamAdapter str((String&)response);
		BinaryStream bin(str);
			
		bin.Write(status);

		Iterand<String> it = data.Forward();
		while(it)
		{

			bin.Write(it().Size);
			bin.Write(it());	
			++it;				
		}
		
		/*
		OutputConsole("WriteResponse - Status: %d\n",status);
		for(Iterand<String> it = data.Forward();it != 0;++it)
		{
			OutputConsole("Data: (%d) %s\n",it().Size,it().Print());				
		}
		OutputConsole("\n\n");			
		*/


		return true;
	}
	

	
	bool Has(String & query, String & ns, String & data)
	{
		// has/key/namespace/2311241
		// has/hash/namespace/65787b6c48968f2c3a3c7464585e56c3
		// has/value/namespace/{array:[1,2,3,4]}
		
	}
		
	bool Get(const String & ns, const String & key, String & value)
	{
		// If there was no get, release the value so that its null
		value.Release();

		Iterand< Mapped<String,Store> > nsit = Namespaces.Contains(ns);
		if (nsit)
		{
			//OutputConsole("Client::Get - Selecting key: %s\n",key.Print());

			return nsit().Value().Select(key,value);
		}
		
		return false;
	}
	
	bool Put(const String & ns, const String & key, const String & value)
	{
		Iterand< Mapped<String,Store> > nsit = Namespaces.Select(ns);
		if (!nsit)
		{
			// Be a bit careful, cant use the update method or we wipe out the whole
			// value store with a new temporary default constructed one.  Ouch.
			nsit = Namespaces.Insert(ns);
		}

		if (nsit)
		{
			return nsit().Value().Insert(key,value);
		}
		
		return false;
		
	}
	
	bool Cur(const String & ns, String & key, String & value)
	{
		// FIXME
		// This is a slow way of cursoring records, especially if random access
		// is supported by the underlying key value store.

		Iterand< Mapped<String,Store> > nsit = Namespaces.Select(ns);
		if (nsit)
		{
			Auto<KeyValueIterator *> it = nsit().Value().Forward();
			if (it)
			{
				if (key.IsEmpty() || it->Move(key))
				{
					key = it->Key();
					value = it->Value();
					return true;
				}
			}
		}
		
		// Clear the key, since there is nothing more to iterate
		key.Release();
		value.Release();
		return false;
	}

	bool Idx(const String & ns, int index, String & key, String & value)
	{

		Iterand< Mapped<String,Store> > nsit = Namespaces.Select(ns);
		if (nsit)
		{
			KeyValueStore & kv = nsit().Value();
			if (kv.Access & KeyValueStore::ACCESS_RANDOM)
			{
				kv.Select(index,key,value);
				return true;
			}
		}
		
		key.Release();
		value.Release();
		return false;
	}


	bool Rnd(const String & ns, String & key, String & value)
	{		
		key.Release();
		value.Release();

		Iterand< Mapped<String,Store> > nsit = Namespaces.Select(ns);
		if (nsit)
		{
			KeyValueStore & kv = nsit().Value();

			//OutputConsole("Server::Rnd - Namespace: %s\n", ns.Print());
			
			// If there are no keys, we dont want divide by zero :(
			// But, if there are no keys, theres a bigger problem and actually its good to know about.
			// Also, we have to cast the random number to unsigned before we modulo the keys, or we
			// get a negative number modulo, which then becomes a way bigger number than we actually
			// want.... ouch!
			unsigned long rnd = ((unsigned long)Number::Random()) % kv.Keys();
			
			if (kv.Access & KeyValueStore::ACCESS_RANDOM)
			{
				kv.Select(rnd,key,value);
			}
			else
			{
				// Its a sequential store, so we have to iterate to find the random position
				// this is going to be extremely slow with 50 million keys.  In fact... i have
				// no idea how long it would take to return 1000 keys this way.

				Auto<KeyValueIterator *> it = kv.Forward();
				for (int i=0;i<rnd;++i)
				{
					it->Move();				
				}	
					
				key = it->Key();
				value = it->Value();
			}
			
			//OutputPrint("Server::Rnd - key: %s\n",key.Print());

			return true;
		}
		
		
		// Clear the key, since there is nothing more to iterate
		key.Release();
		value.Release();
		return false;		
	}

	bool Load(const String & host, int port, const String & ns, int limit=0, bool store=false)
	{
		#ifdef REASON_PLATFORM_POSIX

		OutputConsole("Loading data from %s:%d to namespace %s with limit %d\n",((String&)host).Print(),port,((String&)ns).Print(),limit);
		
		int count=0;
		Timer timer;

		RemoteDB db;
		if (!db.open(((String&)host).Print(),port))
		{
			OutputConsole("Error opening database %s\n",db.error().name());
		}


		Iterand< Mapped<String,Store> > nsit = Namespaces.Contains(ns);
		if (!nsit)
		{
			nsit = Namespaces.Insert(ns);		
			OutputConsole("Inserted namespace %s\n",nsit().Key().Print());	
		}



		// A file in case we need to store things directly to disk, for later fast startup
		String name;
		name.Format("data/%s.dat",nsit().Key().Print());
		File file(name);
		FileStream::StreamAdapter fs(file);
		StreamBuffer buf(fs);	
		BinaryStream stream(buf);	
				
		if (store)
		{			
			OutputConsole("Storing directly to disk, no data will be loaded into memory\n");
			
			if (file.Exists())
			{
				name.Format("data/%s.bak",nsit().Key().Print());
				file.Rename(name);
			}	
			
			// Create a new binary file
			file.Open(File::OPTIONS_CREATE_BINARY_MUTABLE);				
		}
		
		if (nsit)
		{
			RemoteDB::Cursor* cur = db.cursor();
			cur->jump();

			std::string k, v;
			while (cur->get(&k, &v, NULL, true))
			{
				// Now resolve from the key to the md5 hash and get the data
					
				if (store && file.IsWriteable())
				{	
					++count;
								
		

					// Write the size of the record, and ouch avoid the stl 
					// size_t being bigger on 64 bit bug...
					int crc = k.size()+v.size()+8;
					stream.Write(crc);
					stream.Write((int)k.size());
					stream.Write((char*)k.c_str(),(int)k.size());
					stream.Write((int)v.size());
					stream.Write((char*)v.c_str(),(int)v.size());
					
				}
				else
				{
					
					String key((char*)k.c_str(),(int)k.size());
					String value((char*)v.c_str(),(int)v.size());
					
					nsit().Value().Insert(key,value);			
				}
				

				if (limit > 0 && --limit == 0)
					break;
			}

			OutputConsole("Loaded %d keys in %g seconds.\n",count,timer.ElapsedSeconds());

			delete cur;
		
		}

		if(store)
		{	
			buf.Flush();
			file.Close();
		}

		// close the database
		if (!db.close())
		{
			OutputConsole("Failed to close databases, error: %s",db.error().name());
			return false;
		}

		return true;
		
		#else
		// Not supported on windows
		return false;
		#endif

	}
	
	bool Sort(const String & ns="",int limit=0)
	{
		OutputConsole("Sorting data files!\n");

		long long keys = 0;
		long long values = 0;

		Timer timer;
		long total = 0;

		// Use a temprary array store, to map from key to file offset for each record
		// then sort the keys and write a new file using the offset to seek.
		
		ArrayStore store;
		store.KeyValue.Initial->Base->Order.Option = Orderable::ORDER_DEFAULT;
		

		String name;
		name = "./data/";		
		Reason::System::Folder folder(name);
		folder.List();
		
		Iterator<File> files = folder.Files.Iterate();
		files.Forward();
		while(files.Has())
		{
			long count = 0;
			if (files().Extension().Is("dat"))
			{
				String fns = files().Name;
				
				if (!((String &)ns).IsEmpty() && !fns.Is(ns))
					continue;

				OutputConsole("Namespace: %s\n",fns.Print());

				// Create a file so we can manually open it in mutable binary mode
				// instead of the implied read mode when the stream tries to read.
				File input = files();
				input.Open(File::OPTIONS_OPEN_BINARY_READ);
				
				
				FileStream::StreamAdapter ifs(input);
				StreamBuffer ibuf(ifs);	
				BinaryStream istream(ibuf);	
				while (istream.IsReadable() && !input.Error())
				{
					// Use the total size stored at the beginning to verify the reading process
					
					//OutputConsole("%d\n",count);

					int offset=istream.Position;
					
					int crc=0;					
					String key;
					String value;
					
					Server::ReadStream(istream,crc,key,value);

					// Store the offset as the value, so we can write out a sorted file
					value = offset;
					store.Insert(key,value);
					
					if ((count%100000) == 0)
					{
							OutputConsole("Read %ld keys/values\n",count);
					}


					keys += key.Size;
										
					++count;
					
					if (limit > 0 && count >= limit)
						break;
				}

				OutputConsole("Loaded %d offsets into key/value store.\n",count);

				OutputConsole("Sorting key/values with offsets.\n");
				store.KeyValue.Sort();

				OutputConsole("Writing sorted results.\n");

				// Now create a new sorted file.
				String buffer;

				File output = name + ns + ".srt";
				output.Open(File::OPTIONS_CREATE_BINARY_MUTABLE);
				
				FileStream::StreamAdapter ofs(output);

				// FIXME
				// The seek on this buffered stream does not work correctly
				StreamBuffer obuf(ofs);	
				BinaryStream ostream(obuf);	
				
				//BinaryStream ostream(ofs);	

				KeyValueIterator * it = store.Forward();
				while(it->Has())
				{

					String key = it->Key();
					String value = it->Value();

					//OutputConsole("Writing key %s\n",key.Print());

					int offset = value.Integer();
					
					// Now read the crc/size and write the size followed by the entire chunk
					int size=0;
					istream.Seek(offset);
					istream.Read(size);					
					ostream.Write(size);
					
					istream.Read(buffer,size);
					ostream.Write(buffer);
				
					it->Move();
				}

				delete it;
			}	
			
			total += count;
			files.Move();	
		}

		return true;
	}
	
	struct LoadOptions
	{
		int Limit;
		int Count;
		long long Keys;
		long long Values;
		Reason::System::File File;

		LoadOptions(int limit, Reason::System::File & file):Limit(limit),Count(0),Keys(0),Values(0),File(file) {}
	};	
	
	void LoadParallel(void * pointer)
	{
		Fibre * fibre = (Fibre*) pointer;
		LoadOptions * opts = (LoadOptions*)fibre->Context;
		OutputConsole("Server::LoadParallel - Starting parallel load on thread %08lX with opts(%d,%s)\n",fibre->Thread.Id,opts->Limit,opts->File.Name.Print());
		Load(opts);
		OutputConsole("Server::LoadParallel - Finished parallel load on thread %08lX\n",fibre->Thread.Id);
	}
	
	bool Load(LoadOptions * opts)
	{
		File file = opts->File;
		String ns = file.Name;
		
		this->Lock();
		Iterand< Mapped<String,Store> > nsit = Namespaces.Update(ns);
		this->Unlock();
		
		#ifdef KRAKEN_USING_ARRAYSTORE
		// Temporarily disable sorted input in the store
		ArrayStore * store = &(nsit().Value());
		store->KeyValue.Order = Orderable::ORDER_DEFAULT;
		//DoubleArrayStore * store = &(nsit().Value());
		//store->KeySet.Order = Orderable::ORDER_DEFAULT;
		//store->ValueSet.Order = Orderable::ORDER_DEFAULT;
		#endif

		#ifdef KRAKEN_USING_OFFSETSTORE
		OffsetStore * store = &(nsit().Value());
		#endif


		OutputConsole("Namespace: %s\n",ns.Print());
		
		// By tracking the last key and comparing to the current we can determin if 
		// we have loaded a sorted file or not with zero overhead.  Thus avoiding the
		// all important N^2 worst case performance of the parallel quicksort.
		bool sorted = true;
		String sort;

		// Create a file so we can manually open it in mutable binary mode
		// instead of the implied read mode when the stream tries to read.
		//File file = files();
		file.Open(File::OPTIONS_OPEN_BINARY_MUTABLE);
		
		FileStream::StreamAdapter fs(file);
		
		
		// Depending on whether the file is compresed we will use one of these
		// two streams.  This is not a great interface admitedly...
		StreamBuffer buf(fs);
		GzipStream gz(fs);
			
		
		Auto<BinaryStream*> stream;
		
		if (file.Extension().Is("gz"))
			stream = new BinaryStream(gz);
		else
			stream = new BinaryStream(buf);
		
		while (stream->IsReadable() && !file.Error())
		{
			// Use the total size stored at the beginning to verify the reading process
			
			int crc=0;					
			String key;
			String value;

			int read = Server::ReadStream(*stream,crc,key,value);
			if (read == 0)
			{
				if (!file.Eof())
				{
					OutputConsole("Could not read CRC but no end of file found, aborting\n");
					return false;
				}
				break;
			}
			
			if (sorted && !sort.IsEmpty() && sort.Compare(key) > 0)
			{
				// If the previous key was greater than the current key, then we are not
				// in ascending sorted order and we will need to run a parallel sort.
				sorted = false;
			}
			else
			{
				sort = key;
			}
			
			//OutputConsole(" %d ",count);
			
			if ((opts->Count%100000) == 0)
			{
					OutputConsole("Read %ld keys/values\n",opts->Count);
			}

			opts->Keys += key.Size;
		
			if (!nsit().Value().Insert(key,value))
			{
				OutputConsole("Could not insert key/value with key %s\n",key.Print());
			}
			
			++opts->Count;
			
			if (opts->Limit > 0 && opts->Count >= opts->Limit)
				break;
		}

		
		#ifdef KRAKEN_USING_ARRAYSTORE
		store->KeyValue.Order = Orderable::ORDER_ASCENDING;
		if (!sorted)
			store->KeyValue.Sort();
		#endif

		#ifdef KRAKEN_USING_OFFSETSTORE
		store->Sort(sorted,this->Verify);
		#endif	
		
		return true;
	}
	
	
	
	bool Load(int limit=0)
	{
		OutputConsole("Loading data from disk with limit %d\n",limit);
		
		// Track the size in bytes of keys and values we actually load, this
		// helps to predict memory usage.
		long long keys = 0;
		long long values = 0;

		// Load the in memory database from disk
		Timer timer;
		long count = 0;

		String name;
		name = this->Path;		
		Reason::System::Folder folder(name);
		folder.List();
		
		OutputConsole("%s\n",folder.Print());
		OutputConsole("%s\n",folder.Path.Print());
		OutputConsole("%s\n",folder.Name.Print());
		
		Reason::Structure::List<Fibre> fibres;
				
		Iterator<File> files = folder.Files.Iterate();
		files.Forward();
		while(files.Has())
		{
			long count = 0;
			if (files().Extension().Is("dat") || files().Extension().Is("gz"))
			{
			
				// Make this multithreaded... to reduce load time.
				Fibre fibre(Callback<void,void*>(this,&Server::LoadParallel),new LoadOptions(limit,files()));	
				fibre.Start();	
				
				// Start the fibre before copying it, or the thread wont get the
				// id and there will be a segfault when we try to join ! :P
				fibres.Append(fibre);
				
			}
			
			files.Move();		
		}
		
		// Join all the loading threads, and track the stats.
		while (fibres.Count > 0)
		{
			Fibre fibre = fibres[0];
			LoadOptions * opts = (LoadOptions *)fibre.Context;

			// Block until we can join each fibre in turn
			fibre.Join();

			fibres.RemoveAt(0);
			keys += opts->Keys;
			values += opts->Values;
			count += opts->Count;
			delete opts;
		}
		
		OutputConsole("Loaded %d keys in %g seconds.\n",count,timer.ElapsedSeconds());
		OutputConsole("Keys: %ld bytes.\n",keys);
		OutputConsole("Values: %ld bytes.\n",values);
		return true;
	}
	

	
	bool Save()
	{
		OutputConsole("Saving data to disk\n");

		// Store the in memory database back to disk
		int count = 0;
		Timer timer;
		
		Iterand< Mapped<String,Store> > nsit = Namespaces.Forward();
		while(nsit)
		{

			OutputConsole("Namespace: %s\n",nsit().Key().Print());

			String name;
			name.Format("%s/%s.dat",this->Path.Print(),nsit().Key().Print());
		
			OutputConsole("Storing %s\n",name.Print());

			File file(name);
			if (file.Exists())
			{
				name.Format("%s/%s.bak",this->Path.Print(),nsit().Key().Print());
				file.Rename(name);
			}
			
			// Create a new binary file
			file.Open(File::OPTIONS_CREATE_BINARY_MUTABLE);
			
			
			FileStream::StreamAdapter fs(file);
			StreamBuffer buf(fs);	
			BinaryStream stream(buf);		
			
			Auto<KeyValueIterator *> it = nsit().Value().Forward();
			while(it->Has())
			{			
				String key = it->Key();
				String value = it->Value();
				
				++count;
								
				int crc = key.Size+value.Size+8;
				Server::WriteStream(stream,crc,key,value);
				
				it->Move();
			}

			buf.Flush();
			file.Close();
			
			++nsit;
		}
		
		OutputConsole("Saved %d keys in %g seconds.\n",count,timer.ElapsedSeconds());
		return true;
	}
	
	static int ReadStream(BinaryStream & stream, int & crc, String & key, String & value)
	{
		// Unfortunately the stream will say everythings readable until we try to read
		// one byte past the end of the file. So we should always finish on a missing
		// crc record...

		int size = 0;
		int read = 0;
		
		read = stream.Read(crc);
		if (read == 0)
			return 0;
		
		// Reset the read so we can compare it to the crc
		read = 0;		
		read += stream.Read(size);
		//OutputConsole("Size: %d\n",size);
		read += stream.Read(key,size);
		//OutputConsole("Key: %s\n",key.Print());
		read += stream.Read(size);
		//OutputConsole("Size: %d\n",size);
		read += stream.Read(value,size);
		//OutputConsole("Value: %s\n",value.Print());

		if (crc != read)
		{
			OutputConsole("Read inconsistent size from data file at offset %ld, aborting\n",stream.Position);
			for (int i=0;i<100;++i)
			{
				if (stream.IsReadable())
				{
					char byte;
					stream.Read(byte);
					OutputConsole("%02X",byte);
				}							
			}
						
			OutputConsole("\n");
						
			return 0;
		}

		return read;
	}

	static int WriteStream(BinaryStream & stream, int crc, String & key, String & value)
	{
		int write = 0;
		// Write the size of the record
		write += stream.Write(crc);
		write += stream.Write(key.Size);
		write += stream.Write(key);
		write += stream.Write(value.Size);
		write += stream.Write(value);	
					
		/*
		if (count > 98000)
		{
			OutputConsole("Wrote record %d, verifying CRC %d\n",count,crc);
			OutputAssert(stream.Seek(0,0) == stream.Position);
			OutputConsole("Stream: seek %d, position %d\n",stream.Seek(0,0),stream.Position);
						
			int err = 0;
						
			err = file.Error();
			OutputConsole("%s\n",strerror(err));
						
			stream.Seek(-crc,0);

			err = file.Error();
			OutputConsole("%s\n",strerror(err));
						
			int size=0;
			String data;

			stream.Read(size);						
			crc -= 4;

			err = file.Error();
			OutputConsole("%s\n",strerror(err));
						
						
			stream.Read(data,size);
			crc -= data.Size;
			stream.Read(size);
			crc -= 4;
			stream.Read(data,size);
			crc -= data.Size;
			if (crc != 0)
			{
				OutputConsole("CRC failed !!\n");
			}
		}
		*/

		return write;
	}
	
	static int Read(zmq::socket_t & sock, String & data)
	{
		//  Wait for next request from client

		try
		{
			zmq::message_t msg;
			if (!sock.recv(&msg))
			{
				OutputConsole("ZMQ read error: %s\n",strerror(errno));
				return 0;
			}
		
			data.Construct((char*)msg.data(),msg.size());
			return 1;
		}
		catch (zmq::error_t err)
		{
			std::cout << "ZMQ read error: " << err.what() << std::endl;
			data.Release();
			return 0;
		}
	}
	
	static int ReadMultipart(zmq::socket_t & sock, Array<String> & data)
	{
		try
		{
			int m = 0;
			long long o;	// 64 bit socket options
			size_t oo;
			while (true)
			{
				++m;
				zmq::message_t msg;
				if (!sock.recv(&msg))
				{
					OutputConsole("ZMQ read multipart error: %s\n",strerror(errno));
					return 0;
				}	
				data.Append(Superstring((char*)msg.data(),msg.size()));
				sock.getsockopt(ZMQ_RCVMORE,&o,&oo);	
				if (!o)
					break;
			}	
		
			return m;
		}
		catch (zmq::error_t err)
		{
			std::cout << "ZMQ read multipart error: " << err.what() << std::endl;
			data.Release();
			return 0;
		}
	}
	
	static int Write(zmq::socket_t & sock, const String & data)
	{	
		try
		{
			//  Send response back to client
		
			// Send a null message for xreq/xrep routing information	
			//zmq::message_t xmsg(0);		
		    //sock.send(xmsg,ZMQ_SNDMORE);

			zmq::message_t msg(data.Size);
			memcpy ((void *) msg.data(),data.Data,data.Size);
			if (!sock.send(msg))
			{
				OutputConsole("ZMQ write error: %s\n",strerror(errno));
				return 0;
			}
		
			return 1;
		}
		catch (zmq::error_t err)
		{
			std::cout << "ZMQ write error: " << err.what() << std::endl;
			((String&)data).Release();
			return 0;
		}
	}
		
	static int WriteMultipart(zmq::socket_t & sock, Array<String> & data)
	{	
		try
		{
			int m = 0;	
			for (m=0;m<data.Length()-1;++m)
			{
				zmq::message_t msg(data[m].Size);
				memcpy ((void *) msg.data(),data[m].Data,data[m].Size);
				if (!sock.send(msg,ZMQ_SNDMORE))
				{
					OutputConsole("ZMQ write multipart error: %s\n",strerror(errno));
					return 0;
				}			
			}
			++m;	
		
			zmq::message_t msg(data[m].Size);
			memcpy ((void *) msg.data(),data[m].Data,data[m].Size);
			if (sock.send(msg))
			{
				OutputConsole("ZMQ write multipart error: %s\n",strerror(errno));
				return 0;
			}
		
			return m;
		}
		catch (zmq::error_t err)
		{
			std::cout << "ZMQ write multipart error: " << err.what() << std::endl;
			data.Release();
			return 0;
		}
	}

	void Serve(int threads=0)
	{
		
		OutputConsole("Namespaces: %d\n",Namespaces.Length());
		
		Iterand< Mapped<String,Store> > nsit = Namespaces.Forward();
		while(nsit)
		{
			OutputConsole("Namespace: %s\n",nsit().Key().Print());
			//Iterand< Mapped<String,String> > mapit = nsit().Value().Forward();
			OutputConsole("Keys: %d\n",nsit().Value().Keys());
			OutputConsole("Values: %d\n",nsit().Value().Values());
	
			++nsit;
		}
		

		OutputConsole("Serving with %d threads\n",threads);

	   	//  Prepare our context and socket
	    zmq::context_t & context = this->Context;

		if (threads > 0)
		{
			// Run multithreaded with lots of internal sockets
			String server;
			server << "tcp://*:" << this->Port;

			zmq::socket_t clients (context, ZMQ_XREP);
			clients.bind (server.Data);
			//clients.setsockopt(ZMQ_IDENTITY,"K",1);
			
			OutputConsole("Bound to port %s\n",this->Port.Print());
		
			zmq::socket_t workers(context,ZMQ_XREQ);
			workers.bind("inproc://workers");

			while (threads > 0)
			{
				OutputConsole("Starting thread %d\n",threads);
				Start();
				--threads;
			}

			zmq::device (ZMQ_QUEUE, clients, workers);
		}
		else
		{
			// Run single threaded with just one socket

			zmq::socket_t clients (context, ZMQ_XREP);

			zmq_pollitem_t items[1];
			items[0].socket = clients;
			items[0].events = ZMQ_POLLIN;

			while (true)
			{
				int poll = zmq_poll(items,2,-1);
				if (poll < 0)
				{
					OutputConsole("Error %d during poll, aborting\n",poll);
					break;
				}		

				String request;
				String response;

				Server::Read(clients,request);
				Work(request,response);	
				Server::Write(clients,response);

			}

		}
	}

	void Run(void * run)
	{
		Thread * thread = (Thread*)run;
		OutputConsole("Running thread %ld\n",thread->Id);

	    zmq::context_t & context = this->Context;

		zmq::socket_t sock (context, ZMQ_REP);
		sock.connect ("inproc://workers");

	    while (true) 
		{
			try
			{
				String request;
				String response;

				Server::Read(sock,request);
				//OutputConsole("Read request: (%d) %s\n",request.Size,request.Print());

				Work(request,response);

				Server::Write(sock,response);
				//OutputConsole("Write response: (%d) %s\n",response.Size,response.Print());

			}
			catch (std::exception e)
			{
				std::cout << "Std exception: " << e.what() << std::endl;
				break;
			}
		}
	}

	void Work(String & request, String & response)
	{

		bool ok = true;
		String error;
		int method;
		String ns;
		Array<String> data;
		ok = Server::ReadRequest(request,method,ns,data);
		
		if (ok)
		{
			switch (method)
			{
				case Server::METHOD_GET:
				{

					//OutputConsole("METHOD_GET: Getting %d keys/values\n",data.Length());
					String value;
					String key;
					Array<String> keyvalue;
					Iterand<String> it = data.Forward();
					while (it)
					{
						key = it();
						ok &= Get(ns,key,value);
						if (!ok)
							error << Format("METHOD_GET: Failed with ns: %s, key: %s, value: %s\n") << ns << key << value; 

						// Even with get we need to return keys and values to ensure that the right values
						// can be matched with the keys, theres no guarantee that multiple servers and
						// multiple threads on each server process and return their requests in the
						// same order that they arrive.
						keyvalue.Append(key);
						keyvalue.Append(value);						
						//OutputConsole("Value: (%d) %s\n",value.Size,value.Print());

						++it;
					}
					
					//if (ok)
					Server::WriteResponse(response,Server::STATUS_OK,keyvalue);
						
				}		
				break;
				case Server::METHOD_PUT:
				{
					// ...
					
				}
				break;
				case Server::METHOD_CUR:
				{
					// FIXME
					// Ignore the step for now
					
					String key;
					String value;
					Array<String> keyvalue;
					Iterand<String> it = data.Forward();
					while (it)
					{
						key = it();
						// Why not just allow multiple cursors... but then the 
						// request/response would need a count so we could separate
						// the keys list from the values list.  Or just write them
						// as key/value pairs ??
						
						//OutputConsole("METHOD_CUR: Getting next key after %d\n",key.Print());
						
						ok &= Cur(ns,key,value);
						if (!ok)
							error << Format("METHOD_CUR: Failed with ns: %s, key: %s, value: %s\n") << ns << key << value; 

						keyvalue.Append(key);
						keyvalue.Append(value);
						
						++it;
					}
					
					//if (ok)
					Server::WriteResponse(response,Server::STATUS_OK,keyvalue);								
				}
				break;
				case Server::METHOD_RND:
				{
					// Return a random set of keys from the server...
					Iterand<String> it = data.Forward();
					if (it)
					{
						String key;
						String value;						
						int count = it().Decimal();
						Array<String> keyvalue;
						for (int i=0;i<count;++i)
						{							
							// FIXME
							// This ok doesnt work correctly, it should be the other way round, a single error
							// should make it alwasy not ok.
							ok &= Rnd(ns,key,value);
							if (!ok)
								error << Format("METHOD_RND: Failed with ns: %s, key: %s, value: %s\n") << ns << key << value; 

							keyvalue.Append(key);
							keyvalue.Append(value);								
						}
						
						//if (ok)
						Server::WriteResponse(response,Server::STATUS_OK,keyvalue);							
					}
											
				}
				break;
				case Server::METHOD_IDX:
				{
					String key;
					String value;
					Array<String> keyvalue;
					
					Iterand<String> it = data.Forward();
					while (it)
					{
						int index = it().Integer();						
						ok &= Idx(ns,index,key,value);
						if (!ok)
							error << Format("METHOD_IDX: Failed with ns: %s, index: %d, key: %s, value: %s\n") << ns << index << key << value; 

						keyvalue.Append(key);
						keyvalue.Append(value);						
						++it;
					}
					
					//if (ok)
					Server::WriteResponse(response,Server::STATUS_OK,keyvalue);								
				}
				break;
									
			}

		}
		
		
		if (!ok)
		{
			OutputConsole("Error: \n%s\n", error.Print());

			//Array<String> data;
			//data.Append(String("Error"));
			//Server::WriteResponse(response,Server::STATUS_ERROR,data);
		}
		
	}


	
};




////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////


////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////


////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////



////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////


class Client
{
public:

	

	void Run(void * pointer)
	{
		Fibre * fibre = (Fibre*)pointer;
		Arguments & arguments = *((Arguments*)fibre->Context);
	}
	
	
	void Random(const String & ns)
	{		
		// Read blocks of keys over and over...
		
		zmq::context_t context (1);
		zmq::socket_t sock(context, ZMQ_REQ);
		//zmq::socket_t sock(context, ZMQ_XREQ);
		zmq_connect (sock, "tcp://127.0.0.1:5555");
		
		OutputConsole("Connected to server\n");
	
		//int limit = 100000;
		int limit = 10000;	
		Timer timer;
		
		Array<String> keys;
		
		OutputConsole("Using namespace %s\n",((String&)ns).Print());
		
		
		// Request 100 random keys from the server...
		{
			
			Array<String> data;
			data.Append("100");
			
			String request;
			int method = Server::METHOD_RND;
			Server::WriteRequest(request,method,ns,data);
			Server::Write(sock,request);
			
			data.Release();
			
			String response;
			Server::Read(sock,response);
			int status;
			Server::ReadResponse(response,status,data);
			Iterand<String> it = data.Forward();
			while(it)
			{
				String key = it();
				keys.Append(it());
				++it;	// Skip the value
				
				++it;	// Move to next key/value 
			}			
						
		}
		
		OutputConsole("Got 100 random keys/values in %g seconds\n",timer.ElapsedSeconds());	
				
		OutputConsole("=> ");
		
		timer.Start();
		int count = 0;
		while(true)
		{
			//OutputConsole("Request number: %d\n",count);
			
			Array<String> data;
			
			String request;		
			int method = Server::METHOD_GET;
			Server::WriteRequest(request,method,ns,keys);
			Server::Write(sock,request);
			
			
			data.Release();
			
			String response;
			Server::Read(sock,response);
			int status;
			Server::ReadResponse(response,status,data);
						
			if (status != Server::STATUS_OK)
				break;
			
			//OutputConsole("Data: (%d) keys/values\n",data.Length());
			/*
			Iterand<String> it = data.Forward();
			while(it)
			{
				String key = it();
				++it;
				String value = it();
				//OutputConsole("Value: %s\n",value.Print());
				++it;
			}
			*/
			
			if ((count%100) == 0)
			{
				OutputConsole("#");
				OutputFlush();
			}
			
			++count;
			if (count >= limit)
				break;
			
		}
		
	
		
		OutputConsole("\n");
		OutputConsole("Got %d keys/values in %g seconds\n",count*100,timer.ElapsedSeconds());		
		
	}
	
	void Cursor(const String & ns)
	{
		
		//Thread::Sleep(Number::Random()%100);
		//Thread::Sleep(1000);
		
		zmq::context_t context (1);
		
		//  Socket to talk to server
		//printf ("Connecting...\n");
		zmq::socket_t sock(context, ZMQ_REQ);
		//zmq::socket_t sock(context, ZMQ_XREQ);
		
		// This will say its connected even if the server is not bound to that
		// port, weird...  but maybe cool.
		zmq_connect (sock, "tcp://localhost:5555");
		
		OutputConsole("Connected to server\n");
		
		Timer timer;
		
		OutputConsole("Using namespace %s\n",((String&)ns).Print());
		
		
		OutputConsole("=> ");
		
		int count = 0;
		String key = "";
		while(true)
		{
			//printf("Writing request\n");
			
			Array<String> data;
			data.Append(key);
			String request;		
			int method = Server::METHOD_CUR;
			Server::WriteRequest(request,method,ns,data);
			
			
			Server::Write(sock,request);
			
			//printf("Reading response\n");
			
			data.Release();
			
			String response;
			Server::Read(sock,response);
			
			//OutputConsole("%s\n",response.Print());
			//fwrite(response.Data,response.Size,1,stdout);
			
			//if (response.IsEmpty())
			//	break;
			
			//OutputConsole("Request number: %d\n",count);
			
			key.Release();
			
			int status;
			Server::ReadResponse(response,status,data);
			
			//OutputConsole("Status: %d\n",status);
			
			if (status != Server::STATUS_OK)
				break;
			
			Iterand<String> it = data.Forward();
			for(int i=0;it != 0;++i)
			{
				// For this test, just set the key to the first value returned, ignoring 
				// all other key/value pairs.
				if (key.IsEmpty())
					key = it();
				
				//OutputConsole("Data: ");
				
				//StringStream out;
				//Ascii ascii;
				//CodecStream codec(out,ascii);
				//codec.Write(it());
				
				/*
				 if ((i%2) == 0)
				 {
				 // Dont encode the keys
				 OutputConsole("%s\n",it().Print());
				 }
				 else
				 {
				 // Encode the values so we can see that theres output...
				 String out = it();
				 Transcoder trans(out);
				 trans.EncodeBase64();
				 OutputConsole("%s\n",out.Print());
				 }
				 */
				
				++it;
			}
			
			//OutputConsole("\n\n");
			
			if ((count%100) == 0)
			{
				OutputConsole("#");
				OutputFlush();
			}
			
			++count;
			
			//printf("Response: %s\n",response.Print());
			
			//printf("Got value %s for key %d from namespace %s\n",response.Print(),i,ns.Print());
		}
		
		/*
		 for (int i=0;i<100000;++i)
		 {
		 //printf("Writing request\n");
		 
		 String request;
		 request.Format("get/%s/%d",ns.Print(),i);
		 Server::Write(sock,request);
		 
		 //printf("Reading response\n");
		 
		 String response;
		 Server::Read(sock,response);
		 
		 OutputConsole("%s\n",response.Print());
		 
		 if ((i%100) == 0)
		 {
		 OutputConsole("#");
		 OutputFlush();
		 }
		 
		 //printf("Response: %s\n",response.Print());
		 
		 //printf("Got value %s for key %d from namespace %s\n",response.Print(),i,ns.Print());
		 }
		 */
		
		
		OutputConsole("\n");
		
		
		OutputConsole("Got %d values in %g seconds\n",count,timer.ElapsedSeconds());
		
	}	
	
};


////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////


////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////


////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////



////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

int Store::Run(int argc, char * argv[])
{
	if (false)
	{
		int count = 0;
		String path = "data";

		String ipsum;
		File("lorem.txt").Read(ipsum,64);

		Array<String> namespaces = {"test1","test2","test3"};

		Iterand< String > nsit = namespaces.Forward();
		while(nsit)
		{

			OutputConsole("Namespace: %s\n",nsit().Print());

			String name;
			name.Format("%s/%s.dat",path.Print(),nsit().Print());
		
			OutputConsole("Storing %s\n",name.Print());

			File file(name);
			if (file.Exists())
			{
				name.Format("%s/%s.bak",path.Print(),nsit().Print());
				file.Rename(name);
			}
			
			// Create a new binary file
			file.Open(File::OPTIONS_CREATE_BINARY_MUTABLE);
			
			
			FileStream::StreamAdapter fs(file);
			StreamBuffer buf(fs);	
			BinaryStream stream(buf);		
							
			File words("words.txt");			
			String word;

			// This is clearly wrong but it should just read nothing rather than heap overflow under Linux
			// Hard to investigate with the flakey debug environment, ignoring for now.
			//while (file.ReadLine(word))
			while(words.ReadLine(word))
			{							
				String key = word;
				String value = ipsum;
				
				++count;
								
				int crc = key.Size+value.Size+8;
				Server::WriteStream(stream,crc,key,value);				
			}

			buf.Flush();
			file.Close();
			
			++nsit;
		}	
	}

	

	Arguments arguments(argc,argv);
	
	int limit = 100000;
	//int limit = 10000;
	

	if (true)
	{
		Timer timer;
		
		OutputConsole("Arguments: %s\n",arguments.Print());
		
		if (arguments.Select("server"))
		{
			
			Segment * arg;
			arg = arguments.Select("port");
			String port = (arg)?*arg:"5555";
			arg = arguments.Select("path");
			String path = (arg)?*arg:"data";
			
			arg = arguments.Select("threads");
			String threads = (arg)?*arg:"8";

			bool verify = arg = arguments.Select("verify");

			OutputConsole("Starting server with: port %s, path: %s, threads: %s\n",port.Print(),path.Print(),threads.Print());

			Server server(port,path);
			server.Verify = verify;
			
			
			#ifdef REASON_PLATFORM_WINDOWS
			server.Load(500000);
			#else
			server.Load();
			//server.Load(100000);
			//server.Load(1000000);
			//server.Load(4000000);						
			#endif

			//timer.Stop();
			//OutputConsole("Loaded data in %g seconds\n",timer.ElapsedSeconds());

			server.Serve(threads.Integer());

		}
		else
		{
			
			Segment * arg = arguments.Select("ns");
			String ns = (arg)?*arg:"test";
		
			//Fibre fibre(Client,(void*)&arguments);
			//fibre.Start();			
			//fibre.Join();
			
			Client client;
			client.Random(ns);
			//client.Cursor(ns);
			
			//while (true)
			//	Thread::Sleep(1000);
			
		}
		
	}


	if (false)
	{

		// FIXME
		// Ensure that on OSX or other unix systems the ulimit is not set to something low like 256.
		// ulimit -n 8192
		// Or try
		// launchctl limit maxfiles 10240
		// Otherwise zmq will start aborting when it cant create sockets it uses for ipc.
	
		// https://discussions.apple.com/thread/2374157?start=0&tstart=0

		Fibre::Start(ZmqServer);
		Callback<void,void *> client = ZmqClient;
	
		for (int i=0;i<1000;++i)
		{
			Fibre::Start(client);
		}
	
		while(true)
			Thread::Sleep(100);
	}
 
    return 0;
    
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

