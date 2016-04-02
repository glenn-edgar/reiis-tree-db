#
#  File: redis-tree.py
#  This file implements tree structure data bases.  
#  Three structure is managed by encoding the hierachy in strings
#  Only Terminals nodes are stored
#  The redis keys command using grep searches are used to find the hierarchy

import redis
import copy

class Redis_Tree:

   def __init__( self, redis, separator = ":" ):
       self.sep      = separator
       self.redis    = redis
       self.cwd      = []
   
   def get_separator( self ):
       return self.sep

   def _delete_element( self, element):
       if len(element) > 0 :
          del element[-1]
       return element

   #internal class function
   def _expand_directory( self, key_list):
       temp_key_list = copy.copy( self.cwd )
       if ( len(key_list) > 0 ) and (key_list[0] == "/"):
          temp_key_list = []
          self._delete_element( key_list )
       else:
         temp_list = copy.copy( self.cwd )
       for i in key_list:
           if i == ".." :
              self._delete_element(temp_key_list)
           else:
              temp_key_list.append(i)
       return temp_key_list

   # no cwd or special keys like / ..
   def mk_raw_key( self, key_list):
       key_string = self.sep+ self.sep.join( key_list )
       return key_string 

   def mk_key( self, key_list):
       temp_list =  self._expand_directory(key_list)
       key_string = self.sep+ self.sep.join( temp_list )
       return key_string 

   def extract_key( self, key_string):
       key_list = key_string.split(self.sep)
       del key_list[0]
       return key_list

   def get_cwd( self ): # cwd current working directory
       return self.cwd

   def get_full_path( self, key_list):
       return_value = copy.copy(self.cwd)
       return_value.extend( key_list )
       return return_value

   # special strings
   # .. delete previous key
   # /  start from scatch 
   def set_cwd( self, key_list ): 
       self.cwd = self._expand_directory( key_list )
     

   def directory( self, pattern ):
       return_value = set([])
       results = self.directory_full(  pattern )
       for i in results:
           if len(i) > 0:
               return_value.add(i[0])
       return list(return_value)
                     
   def _process_directory_results( self, key_list ):
       result = []
       for i in key_list:
         temp = self.extract_key( i )
         temp_1 = temp[len(self.cwd):]
         result.append( temp_1)
       return result

   
   def directory_full( self,pattern):
       temp_pattern    = self._expand_directory( pattern)
       search_string   = self.sep+ self.sep.join( temp_pattern ) + "*"
       key_list = self.redis.keys( search_string )
       return self._process_directory_results( key_list)


   def delete_all( self, pattern ):
       temp_pattern    = self._expand_directory( pattern)
       search_string   = self.sep+ self.sep.join( temp_pattern ) + "*"
       key_list = self.redis.keys( search_string )

       for i in key_list:
           self.redis.delete(i)

    
 
if __name__ == "__main__":
   # test driver
   redis  = redis.StrictRedis( host = "127.0.0.1", port=6379, db = 11 )   
   redis_tree = Redis_Tree( redis )
   print redis_tree.get_separator()
   print redis_tree.mk_key(["this","is","a","test"])
   print redis_tree.cwd


    
   #
   #  storing ref object
   #
   #
   key = []
   key.append(["1","2","A"])
   key.append(["1","2","B"])
   key.append (["1","2","C"])
   level = []
   level.append( ["1"] )
   level.append(["1","2"])

   # store keys
   for i in key:
      temp_string = redis_tree.mk_key( i)
      print i, i[2], temp_string
      redis.set(temp_string,i[2])
      print redis_tree.extract_key( temp_string )
   print redis.keys("*")
   print redis_tree.directory_full( [] )
   print redis_tree.directory( [] )
   redis_tree.delete_all( [] )
   print redis_tree.directory_full( [] )

   # now do cwd management
   # repopulate redis db
   
   print "repopulating data base" 
   for i in key:
      temp_string = redis_tree.mk_key( i)
      print i, i[2], temp_string
      redis.set(temp_string,i[2])
   print "current db"   
   print redis.keys("*")
   print "testing 1 level"
   redis_tree.set_cwd(["1"])
   print redis_tree.get_cwd()
   print redis_tree.directory_full( [] )
   print redis_tree.directory( [] )
   print "two level"
   redis_tree.set_cwd(["2"])
   print redis_tree.get_cwd()
   print redis_tree.directory_full( [] )
   print redis_tree.directory( [] )

   print "testing full path"
   full_dir = redis_tree.directory_full([])
   for i in full_dir:
      print redis_tree.mk_raw_key( redis_tree.get_full_path(i) ) 


   print "testing .."
   redis_tree.set_cwd([".."])
   print redis_tree.get_cwd()
   print redis_tree.directory_full( [] )
   print redis_tree.directory( [] )

   print "testing /"
   redis_tree.set_cwd(["/"])
   print redis_tree.get_cwd()
   print redis_tree.directory_full( [] )
   print redis_tree.directory( [] )
   print "deleting data base"
   redis_tree.delete_all( [] )
