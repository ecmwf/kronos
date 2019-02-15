
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <netdb.h>

#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include <errno.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <assert.h>

#include "kronos/json.h"
#include "io_task.h"

#include "logger.h"

/* max num of connection requests allowed */
#define MAXNREQ 1024

#define JSON_BUF_LEN 1024
#define IO_DATA_BUF 1048576
#define MAX_WRITE_SIZE 1048576



int do_write_to_file(int fd,
                     const long int* size,
                     long int* actual_number_writes) {

    long remaining;
    long chunk_size;
    int result;
    char* buffer = 0;
    long int bytes_to_write;

    /* Loop over chunks of the maximum chunk size, until all written */
    remaining = *size;
    while (remaining > 0) {

        chunk_size = remaining < MAX_WRITE_SIZE ? remaining : MAX_WRITE_SIZE;
        assert(chunk_size > 0);
        DEBG2("Writing chunk of: %li bytes", chunk_size);

        buffer = malloc(chunk_size);

        bytes_to_write = chunk_size;
        while (bytes_to_write > 0) {

            DEBG2("----> writing: %li bytes", bytes_to_write);
            result = write(fd, buffer, bytes_to_write);
            DEBG2("----> result: %i", result);
            (*actual_number_writes)++;


            if (result == -1) {
                ERRO3("A write error occurred: %d (%s)", errno, strerror(errno));
                return -1;
                break;
            }

            bytes_to_write -= result;
        }

        free(buffer);
        buffer = 0;
        remaining -= chunk_size;
    }

    return 0;
}



int write_file(const char* file_name,
               const long int* n_bytes,
               const long int* offset,
               const long int* n_writes,
               const char* write_mode){

    int fd;
    int open_flags;
    int iwrite;
    long int actual_writes = 0;
    long int per_write_size = *n_bytes / *n_writes;

    DEBG1("--> executing writing.. ");

    open_flags = O_WRONLY | O_CREAT;
    if (!strcmp(write_mode, "append")){
        open_flags = O_WRONLY | O_CREAT | O_APPEND;
    }

    DEBG2("opening file %s", file_name);
    fd = open(file_name, open_flags, S_IRUSR | S_IWUSR | S_IRGRP);

    if (fd == -1) {
        ERRO2("An error occurred opening the file %s for write: %d (%s)", file_name);
        return -1;
    }

    /* do the writing (NOTE: offset=0 at the moment).. */
    DEBG2("writing a total of %li bytes", *n_bytes);
    for (iwrite=0; iwrite<*n_writes; iwrite++){

        DEBG2("writing chunk of %li bytes", per_write_size);
        do_write_to_file(fd, &per_write_size, &actual_writes);
    }

    /* close the file */
    if (close(fd) == -1){
        ERRO2("An error occurred closing the file %s: %d (%s)", file_name);
        return -1;
    } else {
        DEBG2("file %s closed.", file_name);
    }

    return 0;

}


int read_file(const char* file_name,
              const long int* n_bytes,
              const long int* n_reads,
              const long int* offset){

    FILE* file;
    long result;
    int ret;
    long int iread;
    long int read_chunk = *n_bytes / *n_reads;
    char* read_buffer;

    DEBG1("--> executing reading.. ");

    read_buffer = malloc(*n_reads * read_chunk);

    /* open-read-close */
    for (iread=0; iread<*n_reads; iread++)
    {
        file = fopen(file_name, "rb");
        if (file != NULL) {

            ret = fseek(file, *offset, SEEK_SET);

            if (ret == -1) {
                ERRO2("A error occurred seeking in file: %s (%d) %s", file_name);
            } else {

                result = fread(read_buffer, 1, read_chunk, file);

                if (result != read_chunk) {
                    ERRO2("A read error occurred reading file: %s (%d) %s", file_name);
                }
            }

            fclose(file);
        }

    }

    free(read_buffer);

    return 0;

}



execute_io_task(const char * io_json_str, const char * io_data){

    JSON* _json;
    JSON* _jtmp;
    IOTask* iotask;

    const char* _io_task_type;
    long int _io_task_bytes;
    long int _io_task_nwrites;
    long int _io_task_nreads;
    const char* _io_task_file;
    long int _io_task_offset;
    const char* _io_task_mode;

    INFO1("======= executing IO ========");

    iotask = malloc(sizeof(IOTask));

    iotask_fromstring(iotask, io_json_str);
    INFO2("iotask type: %s", get_iotask_type(iotask));
    free(iotask);

    _json = json_from_string(io_json_str);

    json_as_string_ptr(json_object_get(_json, "type"), &_io_task_type);
    DEBG2("IO type: %s", _io_task_type);

    json_object_get_integer(_json, "bytes", &_io_task_bytes);
    DEBG2("IO bytes: %li", _io_task_bytes);

    json_as_string_ptr(json_object_get(_json, "file"), &_io_task_file);
    DEBG2("IO file: %s", _io_task_file);

    json_object_get_integer(_json, "offset", &_io_task_offset);
    DEBG2("IO offset: %li", _io_task_offset);

    /* n_writes only present for writing tasks */
    if (json_object_has(_json, "write_mode")){
        json_object_get_integer(_json, "n_writes", &_io_task_nwrites);
        DEBG2("IO n writes: %li", _io_task_nwrites);
    }

    /* write mode only present for writing tasks */
    if (json_object_has(_json, "write_mode")){
        json_as_string_ptr(json_object_get(_json, "write_mode"), &_io_task_mode);
        DEBG2("IO mode: %s", _io_task_mode);
    }

    /* n_reads only present for reading tasks */
    if (!strcmp(_io_task_type, "reader")){
        json_object_get_integer(_json, "n_reads", &_io_task_nreads);
        DEBG2("IO n reads: %li", _io_task_nreads);
    }


    if (!strcmp(_io_task_type, "writer")){

        write_file(_io_task_file, &_io_task_bytes, &_io_task_offset, &_io_task_nwrites, _io_task_mode);

    } else if (!strcmp(_io_task_type, "reader")) {

        read_file(_io_task_file, &_io_task_bytes, &_io_task_nreads, &_io_task_offset);
    }


    return 0;
}





int main(int argc, char **argv) {

  int parentfd; /* parent socket */
  int childfd; /* child socket */
  int portno; /* port to listen on */
  socklen_t clientlen; /* byte size of client's address */
  struct sockaddr_in serveraddr; /* server's addr */
  struct sockaddr_in clientaddr; /* client addr */
  struct hostent *hostp; /* client host info */

  char iotask_msg_buffer[JSON_BUF_LEN]; /* message buffer */
  char io_data[IO_DATA_BUF];
  int errno_io;

  char *hostaddrp; /* dotted decimal host addr string */
  int optval; /* flag value for setsockopt */
  int n; /* message byte size */
  const char *ack_msg = "message-processed";
  const char *kill_msg = "terminate-server";


  /*
   * check command line arguments
   */
  if (argc != 2) {
    ERRO2("usage: %s <port>", argv[0]);
  }
  portno = atoi(argv[1]);

  /*
   * socket: create the parent socket
   */
  parentfd = socket(AF_INET, SOCK_STREAM, 0);
  if (parentfd < 0)
    ERRO1("ERROR opening socket");

  /* setsockopt: Handy debugging trick that lets
   * us rerun the server immediately after we kill it;
   * otherwise we have to wait about 20 secs.
   * Eliminates "ERROR on binding: Address already in use" error.
   */
  optval = 1;
  setsockopt(parentfd, SOL_SOCKET, SO_REUSEADDR,
             (const void *)&optval , sizeof(int));

  /*
   * build the server's Internet address
   */
  memset((char *) &serveraddr, 0, sizeof(serveraddr));

  /* this is an Internet address */
  serveraddr.sin_family = AF_INET;

  /* let the system figure out our IP address */
  serveraddr.sin_addr.s_addr = htonl(INADDR_ANY);
  INFO3("Server running on host: %s, port:%i",
         serveraddr.sin_addr.s_addr,
         portno);


  /* this is the port we will listen on */
  serveraddr.sin_port = htons((unsigned short)portno);

  /*
   * bind: associate the parent socket with a port
   */
  if (bind(parentfd, (struct sockaddr *) &serveraddr, (socklen_t)sizeof(serveraddr)) < 0) {
    ERRO1("ERROR on binding");
  }

  /*
   * listen: make this socket ready to accept connection requests
   */
  if (listen(parentfd, MAXNREQ) < 0)
    ERRO1("ERROR on listen");

  /*
   * main loop: wait for a connection request, echo input line,
   * then close connection.
   */
  clientlen = sizeof(clientaddr);
  INFO1("Server running..");
  while (1) {

    /*
     * accept: wait for a connection request
     */
    childfd = accept(parentfd, (struct sockaddr *) &clientaddr, &clientlen);
    if (childfd < 0) {
      ERRO1("ERROR on accept");
    }

    /*
     * gethostbyaddr: determine who sent the message
     */
    hostp = gethostbyaddr((const char *)&clientaddr.sin_addr.s_addr,
                          sizeof(clientaddr.sin_addr.s_addr), AF_INET);
    if (hostp == NULL) {
      ERRO1("ERROR on gethostbyaddr");
    }

    hostaddrp = inet_ntoa(clientaddr.sin_addr);
    if (hostaddrp == NULL) {
      ERRO1("ERROR on inet_ntoa");
    }

    INFO3("server established connection with %s (%s)", hostp->h_name, hostaddrp);

    /* receive one message only */
    memset(iotask_msg_buffer, 0, JSON_BUF_LEN);
    n = read(childfd, iotask_msg_buffer, JSON_BUF_LEN);
    if (n < 0)
      perror("ERROR reading from socket");

    printf("server received %d bytes, msg: %s", n, iotask_msg_buffer);

    if (!strcmp(iotask_msg_buffer, kill_msg)){
        INFO2("received kill signal %s => terminating.", iotask_msg_buffer);
        exit(EXIT_SUCCESS);
    }

    /*
     * acknowledge the client for reception
     */
    n = write(childfd, ack_msg, strlen(ack_msg));
    if (n < 0) {
      FATL1("ERROR writing to socket");
    }

    /* perform the IO task as requested */
    errno_io = execute_io_task(iotask_msg_buffer, io_data);

    /* send data back (only if it's a reading instruction)
    if (!errno_io){
       n = write(childfd, ack_msg, strlen(ack_msg));
       if (n < 0)
         perror("ERROR writing to socket");
    } */


    /* close connection */
    close(childfd);


  } /* server loop */

}


