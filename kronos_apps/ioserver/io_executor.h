

/* do write on file */
int _do_write_to_file(int fd,
                     const long int* size,
                     long int* actual_number_writes);



/* open-write-close file */
int _write_file(const char* file_name,
               const long int* n_bytes,
               const long int* offset,
               const long int* n_writes,
               const char* write_mode);


/* open-read-close */
int _read_file(const char* file_name,
              const long int* n_bytes,
              const long int* n_reads,
              const long int* offset);


/* Execute an I/O task */
int execute_io_task(const char * io_json_str, const char * io_data);
