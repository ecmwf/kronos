#include <iostream>
#include <fstream>
#include <unistd.h>

//---include timing funct..
#include <ctime>

#include <math.h>

#include "stdlib.h"
#include "stdio.h"
#include "string.h"
#include "sys/times.h"
#include "sys/vtimes.h"


#include <boost/numeric/ublas/matrix.hpp>
#include "mpi.h"


// #define TMAX 20.
// #define NWRITE 5
// #define KB_WRITE 100000
// #define FILE_WRITE_NAME "example_out.csv"


typedef boost::numeric::ublas::matrix< double, boost::numeric::ublas::column_major, std::vector<double> > matrix_t;


int main(int argc, char * argv[])
{
  
  //------------ MPI init --------------
  int irank;
  int nrank;
  
  MPI_Init(&argc, &argv);
  MPI_Comm_size(MPI_COMM_WORLD, &nrank);
  MPI_Comm_rank(MPI_COMM_WORLD, &irank);
  //------------------------------------
  
  //--------- init matrices ------------
  double cpu_pc;
  
  matrix_t A;   ///< A matrix in C = A * B
  matrix_t B;   ///< B matrix in C = A * B
  matrix_t C;   ///< C matrix in C = A * B
    
  int m = 10;
  int n = 50;
  int k = 30;
  
  A.resize (m,k);
  B.resize (k,n);
  C.resize (m,n);

  A *= 0.;
  B *= 0.;
  C *= 0.;    
  //---------------------------------------
  
  //------------- init params -------------
  clock_t t0;
  clock_t tt;
  float time;
  double inter_t;
  bool hasIOBeenDone_flag;
  //---------------------------------------
    
    
  //--------- open output file ------------
  std::ofstream outfile;
  //---------------------------------------
  
  
  t0 = clock();
  tt = clock();
  time = double(tt - t0) / CLOCKS_PER_SEC;

  while(time < TMAX)
  {
    
    //--------------- time -----------------
    time = double(tt - t0) / CLOCKS_PER_SEC;
    //--------------------------------------
    
    //--- BLAS matrix mult Block -----
    C = prod( A , B );
    //--------------------------------    
      
    if(!irank && !hasIOBeenDone_flag && time>TMAX/2.)
    {
      
	//-------- write I/O ---------
	if(NWRITE)
	{
	  FILE * pFileOUT;
	  char buffer[ KB_WRITE ];
	  pFileOUT = fopen ( FILE_WRITE_NAME, "wb");
	  
	  for(int iW=0; iW<NWRITE; iW++)
	  {
	    fwrite (buffer , sizeof(char), sizeof(buffer), pFileOUT);
	    fflush(pFileOUT);
	  }
	  fclose (pFileOUT);
	}
	//----------------------------
	
	//--------- read I/O ---------
	if(NREAD)
	{
	  FILE * pFileIN;
	  char buffer[ KB_READ ];
	  pFileIN = fopen ( FILE_READ_NAME, "rb");
	  
	  for(int iW=0; iW<NREAD; iW++)
	  {
	    fread (buffer , sizeof(char), sizeof(buffer), pFileIN);
  // 	  fflush(pFileIN);
	  }
	  
	  fclose (pFileIN);
	}
	//----------------------------	
	
	hasIOBeenDone_flag = true;
    }
    
    tt = clock();

  }//-- end main loop
  
  
  
  //---- close file ------  
  if(!irank)
  {
    outfile.close();
    std::cout << "job has finished" << std::endl;    
  }
  //----------------------
  
  
  //---- mpi finalize ----
  MPI_Finalize();
  //----------------------
  
  return 0;
  
}


