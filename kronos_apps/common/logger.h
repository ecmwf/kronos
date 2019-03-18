#ifndef logger_H
#define logger_H


/* log-message max length */
#define LOG_STR_LEN 1024


/* logging message level */
#define LOG_LVL_DEBG 0
#define LOG_LVL_INFO 1
#define LOG_LVL_WARN 2
#define LOG_LVL_ERRO 3
#define LOG_LVL_FATL 4


/* Overall level to show messages:
 * All the messages with level >= output_level will be logged
*/
#define LOG_OUTPUT_LVL_DEFAULT LOG_LVL_INFO


/* logging message level */
#define LOG_LVL_DEBG_STRING "DEBUG"
#define LOG_LVL_INFO_STRING "INFO"
#define LOG_LVL_WARN_STRING "WARNING"
#define LOG_LVL_ERRO_STRING "ERROR"
#define LOG_LVL_FATL_STRING "FATAL"


/* mapping log-level to string */
#define LOG_LVL_STRING(x)  (x == LOG_LVL_DEBG)? LOG_LVL_DEBG_STRING: \
                          ((x == LOG_LVL_INFO)? LOG_LVL_INFO_STRING: \
                          ((x == LOG_LVL_WARN)? LOG_LVL_WARN_STRING: \
                          ((x == LOG_LVL_ERRO)? LOG_LVL_ERRO_STRING: \
                          ((x == LOG_LVL_FATL)? LOG_LVL_FATL_STRING: \
                          "UNKNOWN"))))

#define _LOG_STREAM_STD_DEFAULT 1
#define _LOG_STREAM_ERR_DEFAULT 0
#define _LOG_STREAM_FIL_DEFAULT 0


void log_print(char* filename, int line, int log_level, char *fmt,...);


/* debubg */
#define DEBG() log_print(__FILE__, __LINE__, LOG_LVL_DEBG, "ENTER")
#define DEBG1(x) log_print(__FILE__, __LINE__, LOG_LVL_DEBG, x)
#define DEBG2(x, y) log_print(__FILE__, __LINE__, LOG_LVL_DEBG, x, y)
#define DEBG3(x, y, z) log_print(__FILE__, __LINE__, LOG_LVL_DEBG, x, y, z)
#define DEBG4(w, x, y, z) log_print(__FILE__, __LINE__, LOG_LVL_DEBG, w, x, y, z)

/* info */
#define INFO() log_print(__FILE__, __LINE__, LOG_LVL_INFO, "ENTER")
#define INFO1(x) log_print(__FILE__, __LINE__, LOG_LVL_INFO, x)
#define INFO2(x, y) log_print(__FILE__, __LINE__, LOG_LVL_INFO, x, y)
#define INFO3(x, y, z) log_print(__FILE__, __LINE__, LOG_LVL_INFO, x, y, z)
#define INFO4(w, x, y, z) log_print(__FILE__, __LINE__, LOG_LVL_INFO, w, x, y, z)

/* warning */
#define WARN() log_print(__FILE__, __LINE__, LOG_LVL_WARN, "ENTER")
#define WARN1(x) log_print(__FILE__, __LINE__, LOG_LVL_WARN, x)
#define WARN2(x, y) log_print(__FILE__, __LINE__, LOG_LVL_WARN, x, y)
#define WARN3(x, y, z) log_print(__FILE__, __LINE__, LOG_LVL_WARN, x, y, z)
#define WARN4(w, x, y, z) log_print(__FILE__, __LINE__, LOG_LVL_WARN, w, x, y, z)

/* error */
#define ERRO() log_print(__FILE__, __LINE__, LOG_LVL_ERRO, "ENTER")
#define ERRO1(x) log_print(__FILE__, __LINE__, LOG_LVL_ERRO, x)
#define ERRO2(x, y) log_print(__FILE__, __LINE__, LOG_LVL_ERRO, x, y)
#define ERRO3(x, y, z) log_print(__FILE__, __LINE__, LOG_LVL_ERRO, x, y, z)
#define ERRO4(w, x, y, z) log_print(__FILE__, __LINE__, LOG_LVL_ERRO, w, x, y, z)

/* fatal */
#define FATL() log_print(__FILE__, __LINE__, LOG_LVL_FATL, "ENTER")
#define FATL1(x) log_print(__FILE__, __LINE__, LOG_LVL_FATL, x)
#define FATL2(x, y) log_print(__FILE__, __LINE__, LOG_LVL_FATL, x, y)
#define FATL3(x, y, z) log_print(__FILE__, __LINE__, LOG_LVL_FATL, x, y, z)
#define FATL4(w, x, y, z) log_print(__FILE__, __LINE__, LOG_LVL_FATL, w, x, y, z)





/* ---- just for testing.. ---- */
unsigned short get_start_flag();
unsigned int get_std_flag();
unsigned int get_err_flag();
unsigned int get_fil_flag();
unsigned int get_out_lvl();


#endif

