<?php


namespace kraken\client;


class Client
{

    public $server = 'tcp://127.0.0.1:5555';
	public $ctx	 = null;
    public $sock = null;
	public $proto = null;
    public $uuid = 'kraken_server';
	public $load = 100;
	public $count = 0;
	

    public function __construct($load=0)
    {
        //parent::__construct();
		$this->ctx = new ZMQContext(1);
        $this->sock = new ZMQSocket($this->ctx, ZMQ::SOCKET_XREQ);
        $this->sock->connect($this->server);
        //$this->k_server->setSockOpt(ZMQ::SOCKOPT_IDENTITY, $this->uuid);

		$this->proto = new Protocol();
		
		if ($load != 0)
			$this->load = $load
    }

	public function id()
	{
		$this->count += 1;
		return strval($this->count);
	}
	
	public function read()
	{	
		$msg = $this->sock.recvMulti();
		if strcount($msg) == 3
		{
			$id = $msg[0];
			return $msg[2];
		}
		else
		{
			return null;
		}
	}	
	
	public function write($data)
	{
		# The above was what the documentation said it requried for
		# a XREQ, but the below is what actually works...
		$msg = [$this->id(),"",$data];
		$this->sock.sendMulti(msg);
	}
		
	
	public function get($namespace,$keys=[])
	{
		$values = [];
			
		public function async()
		{
			# Wait for all response messages to be received
			$k = 0;
			$m = 0;
			while ($k < count($keys))
			{
				$req = $this->proto.get($namespace,array_slice($keys,$k,$k+$this->load));
				$this->write($req);
				$k += $this->load;
				$m += 1;
			}
				
			while ($m > 0)
			{
				$res = $this->read();
				#print "Received res: %s" % binascii.hexlify(res)
				$data = [];
				$status = $this->proto.response($res,$data);
				array_push($values,$data);
				$m -= 1;
			}
		}
		
		async()
		
		return $values
	}
		
	public function put($namespace,$keyvalue={})
	{
		# Supply a map of key/value pairs
	}
		
	public function cur($namespace,$keys=[])
	{
		# Set the key to initiate cursor if none is specified
		if (count($keys) == 0)
			$keys = [""]

		$req = $this->proto.cur($namespace,$keys)		
		#print "Sending req: %s" % binascii.hexlify(req)
		$this->write($req)
		$res = $this->read()
		#print "Received res: %s" % binascii.hexlify(res)
		
		$data = []
		$status = $this->proto.response($res,$data)

		if (count($data) > 1)
			return list($data[0],$data[1])

		return list(null,null)
	}
		
	public function rnd($namespace,$count)
	{	
		map = []
			
		public function async($count)
		{
			$c = 0
			$m = 0
			while ($count > 0)
			{
				if ($count > $this->load)
					$c = $this->load
				else
					$c = $count
					
				$count -= $this->load
				$req = $this->proto.rnd($namespace,$c)
			
				#print "Sending req: %s" % binascii.hexlify(req)
				$this->write($req)
				$m += 1
			}
			
			# Wait for all response messages to be received				
			while ($m > 0)
			{
				$res = $this->read()
				#print "Received res: %s" % binascii.hexlify(res)
				$data = []
				$status = $this->proto.response($res,$data)
				# Convert the flat list of keys/values into a map
				for ($i=0; $i < count($data);i += 2)
				{
					map[$data[$i]] = $data[$i+1]
				}
				$m -= 1
			}
			
			
		async($count)
		
		return map	
	}


}



	
		
class Protocol
{
	
	public static METHOD_GET = 2
	public static METHOD_PUT = 4
	public static METHOD_CUR = 8
	public static METHOD_RND = 16
	
	public static STATUS_ERROR = 0
	public static STATUS_OK = 1
	public static STATUS_EOF = 2
	
    public function __construct()
    {
	}


	publid function request($method,$namespace,$data=[])
    {
		$fmt = sprintf("ii%ss",strlen($namespace));
		$req = pack($fmt,$method,strlen($namespace),$namespace);
		foreach($data as $x)
		{
			$fmt = sprintf("i%ss",strlen($x));
        	$req += pack($fmt,strlen($x),$x);
		}
		return $req
	}
	
	public function response($res,$data=[])
	{
		$status = upack("i",$res);
		# Unpack the length and string data incrementing offset
		$o = 4
		while ($o < strlen($res))
		{
			list(,$l) = unpack("i",substr($res,$o));
			$fmt = sprintf("%ss",$l);
			list(,$s) = unpack($fmt,substr($res,$o+4,$o+4+$l));
			$o += 4+$l;
			array_push($data,$s);
		}
		return status;
	}
		
	public function get($namespace,$keys=[])
	{
		$req = $this->request($this->METHOD_GET,$namespace,$keys);
		return $req
	}
		
	public function put($namespace,$keyvalue={})
	{
		$data = []
		for ($keyvalue as $k=>$v)
		{
			array_push($data,$k);
			array_push($data,$v);
		}
		$req = $this->request($this->METHOD_PUT,$namespace,$data);
		return $req
	}
		
	public function cur($namespace,$keys=[])
	{
		$req = $this->request($this->METHOD_CUR,$namespace,$keys)
		return $req
	}
	
	public function rnd($namespace,$count)
	{
		$req = $this->request($this->METHOD_RND,$namespace,[strval($count)])
		return $req
	}

}


print "hello world";

?>