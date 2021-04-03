
import zmq
from zmq.core import socket

from struct import *
import binascii


# These imports are for testing as is the threaded class
import profile
import threading
import random
import sys

class Threaded(threading.Thread):

	def __init__(self,group=None, target=None, name=None, args=(), kwargs={}):
		threading.Thread.__init__(self)
		self.target = target
		self.lock = threading.Lock()
		self.args = args

	def run(self):
		print self.target
		print "Created thread %s" % self.ident
		self.target(self)
		




class Query:
	
	
	def __init__(self):
		pass
		

class Interface:

	def __init__(self):
		pass

	def get(self,namespace,keys=[]):
		pass

	def cur(self,namespace,keys=[]):
		pass


	def rnd(self,namespace,count):
		pass





class Client:
	
	
	def __init__(self,servers=[],load=100):
		self.proto = Protocol()
		
		#self.ctx = zmq.Context.instance()
		self.ctx = zmq.Context()

		# Use dealer socket to spread connection load over a number of know 
		# clients and break up key requests into blocks of 100 which can be
		# serviced in a distributed fashion using the dealer
		
		# Im not sure how to use the dealer socket, but the req behaves the
		# same with regard to load balanced outgoing messages.
		
		#self.dealer = self.ctx.socket(zmq.DEALER)
		#self.sock = self.ctx.socket(zmq.REQ)

		self.sock = self.ctx.socket(zmq.XREQ)
		#self.sock.setsockopt(zmq.IDENTITY, "X".encode())
		
		self.servers = []
		
		for server in servers:
			self.sock.connect(server)
			#self.servers.append(ctx.socket(zmq.REQ).connect(server))
			
		# http://api.zeromq.org/2-1:zmq-socket
		# http://api.zeromq.org/2-1:zmq-connect
		
		# The load balance factor for queries that can be distributed
		self.load = load
		self.count = 0

	def id(self):
		self.count += 1
		return str(self.count)

	def read(self):
		"""
		while True:
			try:
				msg = self.sock.recv(zmq.NOBLOCK)
			except zmq.ZMQError, e:
				print e
				print e.errno
			else:
				return msg
		"""		
		#return self.sock.recv()
				
		# Custom XREQ/XREP direct connection.
		
		# This is a lazy interpretation, actually there can be many
		# bits of rounting info before msg.index('')-1 though the 
		# basic format is [routing]*[id][''][data] and were only
		# interested in data here.  Its crazy that this is the only 
		# way to multiplex the requests.  
		
		# Very hacky and not obvious, it took me days to work this 
		# out since its not clearly documented.  I should have just
		# read the protocol spec!
		
		msg = self.sock.recv_multipart()
		if len(msg) == 3:
			id = msg[0]
			return msg[2]
		else:
			return None
		
	
	def write(self,data):
		"""
		while True:
			try:
				self.sock.send(data,zmq.NOBLOCK)
			except zmq.ZMQError, e:
				print e
				print e.errno
			else:
				break	
		"""
		
		#self.sock.send(self.id(),zmq.SNDMORE)
		#self.sock.send("",zmq.SNDMORE)		
		#self.sock.send(data)

		# The above was what the documentation said it requried for
		# a XREQ, but the below is what actually works...
		msg = [self.id(),"",data]
		self.sock.send_multipart(msg)
		
	
	def get(self,namespace,keys=[]):
		
		values = []
		
		# Supply a list of keys, get a list of values
		def sync():
			k = 0
			m = 0
			while k < len(keys):
				req = self.proto.get(namespace,keys[k:k+self.load])
				k += self.load
				
				#print "Sending req: %s" % binascii.hexlify(req)				
				self.write(req)			
				res = self.read()
				#print "Received res: %s" % binascii.hexlify(res)

				
				data = []
				status = self.proto.response(res,data)
				values.extend(data)
											
				m += 1
			
		def async():
			# Wait for all response messages to be received
			k = 0
			m = 0
			while k < len(keys):
				req = self.proto.get(namespace,keys[k:k+self.load])
				self.write(req)
				k += self.load
				m += 1
				
			while m > 0:
				res = self.read()
				#print "Received res: %s" % binascii.hexlify(res)
				data = []
				status = self.proto.response(res,data)
				values.extend(data)
				m -= 1				

		
		"""
		req = self.proto.get(namespace,keys)
		self.write(req)
		res = self.read()
		#print "Received res: %s" % binascii.hexlify(res)
		status = self.proto.response(res,values)
		"""
		
		async()
		
		return values
		
	def put(self,namespace,keyvalue={}):
		# Supply a map of key/value pairs
		pass
		
	def cur(self,namespace,keys=[]):
		# Set the key to initiate cursor if none is specified
		if len(keys) == 0:
			keys = [""]

		# Supply a list of keys, get back a list of corresponding 
		# next key/value pairs
		map = {}
		req = self.proto.cur(namespace,keys)
		
		#print "Sending req: %s" % binascii.hexlify(req)
		self.write(req)
		res = self.read()
		#print "Received res: %s" % binascii.hexlify(res)
		
		data = []
		status = self.proto.response(res,data)
		# Convert the flat list of keys/values into a map
		#for i in range(0,len(data),2):
		#	map[data[i]] = data[i+1]
		
		# For simplicity i just want this to return a single pair for now
		#return map
		if (len(data) > 1):
			return (data[0],data[1])

		return (None,None)
		
	def rnd(self,namespace,count):
		
		map = {}
		
		# Supply a count, get back a list of count random
		# key/value pairs. This is actually a pretty expensive
		# query since it requires iterating the data structures
		# in memory to find a random index so split the work up
		# as we do with the get request.
		
		def sync(count):
			c = 0
			m = 0
			while count > 0:
				c = self.load if count > self.load else count
				count -= self.load
				req = self.proto.rnd(namespace,c)
			
				#print "Sending req: %s" % binascii.hexlify(req)
				self.write(req)
				res = self.read()				
				#print "Received res: %s" % binascii.hexlify(res)
				
				data = []
				status = self.proto.response(res,data)
				# Convert the flat list of keys/values into a map
				for i in range(0,len(data),2):
					map[data[i]] = data[i+1]
			
				m += 1
			
		def async(count):
			c = 0
			m = 0
			while count > 0:
				c = self.load if count > self.load else count
				count -= self.load
				req = self.proto.rnd(namespace,c)
			
				#print "Sending req: %s" % binascii.hexlify(req)
				self.write(req)
				m += 1

			# Wait for all response messages to be received				
			while m > 0:
				res = self.read()
				#print "Received res: %s" % binascii.hexlify(res)
				data = []
				status = self.proto.response(res,data)
				# Convert the flat list of keys/values into a map
				for i in range(0,len(data),2):
					map[data[i]] = data[i+1]
				m -= 1
			
			
		async(count)
		
		return map
		
	
		
class Protocol:
	
	METHOD_GET = 2
	METHOD_PUT = 4
	METHOD_CUR = 8
	METHOD_RND = 16
	
	STATUS_ERROR = 0
	STATUS_OK = 1
	STATUS_EOF = 2
	
	def __init__(self):
		pass


	def request(self,method,namespace,data=[]):
		#print namespace,data
		req = pack("ii%ss" % len(namespace),method,len(namespace),namespace)	
		#print binascii.hexlify(request)	
		#print len(request)
		#m,l = unpack_from("ii",request)
		#print unpack_from("%ss" % l,request[8:])
		
		req += "".join(pack("i%ss" % len(x),len(x),x) for x in data)		
		#print request
		#print binascii.hexlify(request)
		
		return req
		
	def response(self,res,data=[]):
		status = unpack_from("i",res)
		# Unpack the length and string data incrementing offset
		o = 4
		while (o < len(res)):
			(l,) = unpack_from("i",res[o:])
			(s,) = unpack_from("%ss" % l,res[o+4:o+4+l])
			o += 4+l
			data.append(s)
		return status
		
	def get(self,namespace,keys=[]):
		req = self.request(self.METHOD_GET,namespace,keys)
		return req
		
	def put(self,namespace,keyvalue={}):
		data = []
		for (k,v) in keyvalue.iteritems():
			data.append(k)
			data.append(v)
		req = self.request(self.METHOD_PUT,namespace,data)
		return req
		
	def cur(self,namespace,keys=[]):
		req = self.request(self.METHOD_CUR,namespace,keys)
		return req
	
	def rnd(self,namespace,count):
		req = self.request(self.METHOD_RND,namespace,[str(count)])
		return req

if __name__ == "__main__":

		
	# Use dealer socket to spread connection load over a number of know 
	# clients and break up key requests into blocks of 100 which can be
	# serviced in a distributed fashion using the dealer.


	#ctx = zmq.Context.instance()	
	#sock = ctx.socket(zmq.DEALER)
	#sock.connect("tcp://10.1.1.30:5555")

	

	def test_read(keys=[]):

		size = 100
		limit = 100000

		sampler = profile.Sampler()

		timer = profile.timer()


		def run(thread=None):	
			
			count = 100000
			
			#count = 10
			counter = profile.timer()
				
			id = 0
			if thread != None:
				print "Running thread %s" % thread.ident
				id = thread.ident

			rt = profile.timer()

			# Every thread gets its own client...
			servers = ["tcp://127.0.0.1:5555"]		
			client = Client(servers)
			
			random.seed(0)
			window = 0
			while True:

				window = int(random.random() * (len(keys)-size))
				if window+size > len(keys):
					window = 0

				kk = keys[window:window+size]
				#print kk
				
				rt.start()

				values = client.get("test1",kk)
				#print len(values)

				rt.stop()

				if values == None or len(values) == 0:
					print "Get failed with keys [%s:%s]" % (window,window+size)
					continue

				#count = count - len(values)
				count -= size
				if (count <= 0):
					break

				#if (count%10000 == 0):
				#	sys.stdout.write("+")
				#	sys.stdout.flush()

				e = rt.elapsed()
				sampler.sample(e)
				
				#print "%s Keys in %s" % (len(map),e)
				#window = window - 1

			print "Count %s" % count
			print "Thread %s in %s" % (id,counter.elapsed())

		def test_read_multithreaded():

			threads = []
			#for t in range(64):
			#for t in range(32):
			for t in range(16):
				thread = Threaded(None,run)		  
				thread.start()
				threads.append(thread)
			
			# Try to join all the threads until theres none left to join
			while len(threads) > 0:
				threads[0].join()
				print "Joined thread %s" % threads[0].ident
				threads.pop(0)

		def test_read_singlethreaded():
			run()
			
		# Load keys from database

		
		servers = ["tcp://127.0.0.1:5555"]		
		client = Client(servers)		
		
		timer.start()
		# You have to send an empty key atleast or the request will not
		# work the way its coded now
		
		# FIXME
		# The cursor could be hundreds of times faster if it returned a hint
		# for the memory address of the node holding the key.
		
		k,v = client.cur("test1",[""])
		#print k
		keys.append(k)
		count = 0
		while k != None:
			k,v = client.cur("test1",[k])
			#print k
			keys.append(k)
			count += 1
			if count%1000 == 0:
				sys.stdout.write("#")
				sys.stdout.flush()
			#print count
			if count >= limit:
				break
		print
		timer.stop()
		
		print "Loaded %s keys in %s" % (len(keys),timer.elapsed())
		random.shuffle(keys)
		#print keys
		
		timer.start()
		#print keys
		test_read_multithreaded()
		#test_read_singlethreaded()

		print "Completed in %s" % timer.elapsed()
		print "Min: %s, Max: %s, Avg: %s" % (sampler.min(),sampler.max(),sampler.avg())
	
	
	
	def test_dealer():
		# Clients can connect to multiple servers and will
		# load balance all requests automatically. Servers
		# should be specified using full zmq uri syntax, so
		# they can be in process, ipc, or tcp.
		
		#servers = ["tcp://10.1.1.30:5555","tcp://10.1.15.212:5555"]
		#servers = ["tcp://10.1.1.30:5555"]
		servers = ["tcp://127.0.0.1:5555"]
		
		client = Client(servers)
		
		# Get 1000 keys at random, then select those random keys
		# again from amonst the connected peers (servers).
		# This should demonstrate the load balancing.
		
		timer = profile.Timer()
		
		timer.start()
		map = client.rnd("test1",1000)
		timer.stop()
		#print map
		print "Got 1000 random key/value in %s" % timer.elapsed()
		
		keys = [key for key in map.iterkeys()]
		timer.start()
		values = client.get("test1",keys)
		timer.stop()
		print values
		print "Got 1000 values in %s" % timer.elapsed()
	
	
	test_read()	
	#test_dealer()
	
	
	
	
