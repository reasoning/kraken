import re
import sys
import os


if __name__ == "__main__":

	print "Kraken build."

	def execute(*args):
		if len(args) == 1 and type(args[0]) is not list:
			args = [args[0]]
		for arg in args:
			#os.system("%s > make.log 2>&1" % arg)
			os.system("%s" % arg)

	def expand(tar):
		os.system("tar zxvf %s > /dev/null" % tar)
		os.chdir(tar[:-len(".tar.gz")])

	def buildReason():

		def buildMySQL():
			print "\t\tBuilding MySQL"
			
			tar = "mysql-connector-c-6.0.2.tar.gz"
			expand(tar)
			execute("cmake -G \"Unix Makefiles\"")
			execute("make")
			execute("rm -rf ../../lib/mysql/include")
			execute("cp -r include ../../lib/mysql/")
			execute("cp libmysql/libmysqlclient* ../../lib/mysql")
			os.chdir("../")

		def buildSQLite():
			print "\t\tBuilding SQLite"
			
			tar = "sqlite-autoconf-3070900.tar.gz"
			expand(tar)
			execute("./configure","make")
			execute("rm ../../lib/sqlite/include/*")
			execute("cp sqlite3.h sqlite3ext.h ../../lib/sqlite/include")
			execute("cp /usr/lib/libsqlite3.a ../../lib/sqlite/")
			os.chdir("../")

		def buildOpenSSL():
			print "\t\tBuilding OpenSSL"
			
			tar = "openssl-1.0.0e.tar.gz"
			expand(tar)
			execute("./config","make")
			execute("rm -rf ../../lib/openssl/include/")
			execute("cp -r include ../../lib/openssl/")
			execute("cp libcrypto.a libssl.a ../../lib/openssl")
			os.chdir("../")

		def buildPostgres():
			print "\t\tBuilding Postgres"

			tar = "postgresql-8.4.9.tar.gz"
			expand(tar)
			execute("./configure","make")
			execute("rm -rf ../../lib/postgres/include/*")
			execute("cp src/interfaces/libpq/*.h ../../lib/postgres/include/")
			execute("cp src/interfaces/libpq/libpq.* ../../lib/postgres/")
			os.chdir("../")

		def buildZlib():
			print "\t\tBuilding Zlib"

			tar = "zlib-1.2.5.tar.gz"
			expand(tar)
			execute("./configure","make")
			execute("rm ../../lib/zlib/include/*")
			execute("cp zlib.h zconf.h ../../lib/zlib/include/")
			execute("cp libz.* ../../lib/zlib/")
			os.chdir("../")

		def buildLzo():
			print "\t\tBuilding Lzo"
	
			tar = "lzo-2.06.tar.gz"
			expand(tar)
			execute("./configure","make")
			execute("rm -rf ../../lib/lzo/include")
			execute("cp -r include ../../lib/lzo")
			execute("cp src/.libs/liblzo2.a ../../lib/lzo")
			os.chdir("../")

		def buildCURL():
			print "\t\tBuilding CURL"
	
			tar = "curl-7.75.0.tar.gz"
			expand(tar)
			execute("./configure","make")
			execute("rm -rf ../../lib/curl/include")
			execute("cp -r include ../../lib/curl")
			execute("cp src/.libs/liblzo2.a ../../lib/lzo")
			os.chdir("../")

		
		print "\tBuilding Reason"

		
		os.chdir("lib")		
		buildMySQL()
		buildSQLite()
		buildOpenSSL()
		buildPostgres()
		buildZlib()
		buildLzo()
		buildCURL()

		execute("make clean","make depend","make library")
		os.chdir("../../")		


	def buildKraken():
		
		def buildZeromq():
			print "\t\tBuilding Zeromq"
			
			os.chdir("zeromq")
			execute("./configure","make","make install")
			execute("cp src/.libs/libzmq.a ./")
			os.chdir("../")

		def buildKyotocabinet():
			print "\t\tBuilding Kyotocabinet"

			os.chdir("kyotocabinet")
			execute("./configure","make","make install")
			os.chdir("../")
		
		def buildKyototycoon():
			print "\t\tBuilding Kyototycoon"

			os.chdir("kyototycoon")
			execute("./configure","make","make install")
			os.chdir("../")

		print "\tBuilding Kraken"
		os.chdir("lib")
		buildZeromq()
		buildKyotocabinet()
		buildKyototycoon()
		execute("make clean","make depend","make")
		os.chdir("../")



	buildReason()
	buildKraken()




		
