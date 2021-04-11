#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/server/TThreadedServer.h>
#include <thrift/transport/TServerSocket.h>
#include <thrift/transport/TBufferTransports.h>
#include <signal.h>

#include "../utils.h"
#include "../utils_mongodb.h"
#include "MovieInfoHandler.h"
#include <mongoc.h>
#include <bson/bson.h>
#include <iostream>

using json = nlohmann::json;
using apache::thrift::server::TThreadedServer;
using apache::thrift::transport::TServerSocket;
using apache::thrift::transport::TFramedTransportFactory;
using apache::thrift::protocol::TBinaryProtocolFactory;

using namespace movies;


// signal handler code
void sigintHandler(int sig) {
	exit(EXIT_SUCCESS);
}

// entry of this service
int main(int argc, char **argv) {
  // 1: notify the singal handler if interrupted
  signal(SIGINT, sigintHandler);
  // 1.1: Initialize logging
  init_logger();


  // 2: read the config file for ports and addresses
  json config_json;
  if (load_config_file("config/service-config.json", &config_json) != 0) {
    exit(EXIT_FAILURE);
  }

  // 3: get my port
  int my_port = config_json["movie-info-service"]["port"];

// Get mongodb client pool
   mongoc_client_pool_t* mongodb_client_pool =
   init_mongodb_client_pool(config_json, "movies", 128);
        	 
 	 	 std::cout << "Mongodb client pool done ..." << std::endl;
   	 	   if (mongodb_client_pool == nullptr) {
	 	        return EXIT_FAILURE;
    	          }
   	 	            
 
   std::cout << "Mongodb before client pop 11..." << std::endl;
    
   mongoc_client_t *mongodb_client = mongoc_client_pool_pop(mongodb_client_pool);
      if (!mongodb_client) {
          LOG(fatal) << "Failed to pop mongoc client";
	        return EXIT_FAILURE;
	    }
//      std::cout << "Mongodb before client pop 22..." << std::endl;

       mongoc_client_pool_push(mongodb_client_pool, mongodb_client);
	std::cout << "Mongodb client push done ..." << std::endl;


   auto collection = mongoc_client_get_collection(
         mongodb_client, "movies", "movie-info");
	  if (!collection) {
 	          ServiceException se;
  	          se.errorCode = ErrorCode::SE_MONGODB_ERROR;
 	          se.message = "Failed to create collection user from DB recommender";
 	          mongoc_client_pool_push(mongodb_client_pool, mongodb_client);
  	          throw se;
 		 }

  	  std::cout << "Mongodb get coll done here !!!!!!!! ..." << std::endl;

//	  std::vector<std::string> movie_ids;
	  std::vector<std::string> movie_titles;
	  movie_titles.push_back("Spidy");
	  movie_titles.push_back("Batty");
//		movie_ids.push_back("MOV0001");
//		movie_ids.push_back("MOV0002");
//		movie_ids.push_back("MOV0003");
//		movie_ids.push_back("MOV0004");


	 mongoc_bulk_operation_t *bulk;
   	 enum N { ndocs = 4 };
     	 bson_t *docs[ndocs];
         bson_error_t error;
	 int i = 0;
	 bool ret;
	 
	 bulk = mongoc_collection_create_bulk_operation_with_opts (collection, NULL);
	 std::cout << "INSEERT 111111111111111111111111111111111111111111 !! ..." << std::endl;

	 bson_t *movie_doc = bson_new();

	 BSON_APPEND_INT64(movie_doc, "movie_id", 1111);
	 BSON_APPEND_UTF8(movie_doc, "title", "Dark Knight");
	// BSON_APPEND_UTF8(movie_doc, "movie_url", "/home/ubuntu/vishh/newmongo/videos/AOSTest.mp4");
	 docs[0] =movie_doc;

	 bson_t *movie_doc1 =  bson_new();
         BSON_APPEND_INT64(movie_doc1, "movie_id", 2222);
         BSON_APPEND_UTF8(movie_doc1, "title", "Spiderman");
	 //BSON_APPEND_UTF8(movie_doc1, "movie_url", "/home/ubuntu/vishh/newmongo/videos/AOSTest.mp4");
         docs[1] =movie_doc1;

	 bson_t *movie_doc2 =  bson_new();
	 BSON_APPEND_INT64(movie_doc2, "movie_id", 3333);
         BSON_APPEND_UTF8(movie_doc2, "title", "The Hangover");
	 //BSON_APPEND_UTF8(movie_doc2, "movie_url", "/home/ubuntu/vishh/newmongo/videos/AOSTest.mp4");
         docs[2] =movie_doc2;

	for (i = 0; i < 3; i++) {
	     mongoc_bulk_operation_insert (bulk, docs[i]);  
	     bson_destroy (docs[i]);
	     docs[i] = NULL;
	     }
	std::cout << "BEFORE BULK DONE !!!!!!! ..." << std::endl;

	   ret = mongoc_bulk_operation_execute (bulk, NULL, &error);

	 if (!ret) {
                  // LOG(error) << "Failed to insert Movies for " << std::to_string(movie_id) << " to MongoDB: " << error.message;
	      std::cout << "BULK insert failed here !!!!!!!! ..." << std::endl;     
	   	 ServiceException se;
	         se.errorCode = ErrorCode::SE_MONGODB_ERROR;
	         se.message = error.message;
	         bson_destroy(movie_doc);
		 bson_destroy(movie_doc1);
		 bson_destroy(movie_doc2);
	             //  mongoc_collection_destroy(collection);
	            //mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);
	         throw se;
	                 }
	      bson_destroy(movie_doc);
	      bson_destroy(movie_doc1);
	      bson_destroy(movie_doc2);
	      std::cout << "BULK DONE !!!!!!! ..." << std::endl;
          	      	

  // 4: configure this server
  TThreadedServer server(
      std::make_shared<MovieInfoServiceProcessor>(
      std::make_shared<MovieInfoServiceHandler>(mongodb_client_pool,mongodb_client)),
      std::make_shared<TServerSocket>("0.0.0.0", my_port),
      std::make_shared<TFramedTransportFactory>(),
      std::make_shared<TBinaryProtocolFactory>()
  );
  
  // 5: start the server
  std::cout << "Starting the movie info server ..." << std::endl;
  server.serve();
  return 0;
}

