#include "pregel_app_edgeFormat.h"

int main(int argc, char* argv[]){
    if(argc != 3)
    {
        cout<<"Convert edge list to adjacency lists\narg1: input path;  arg2: output path"<<endl;
        return -1;
    }
	init_workers();
	convert(argv[1], argv[2]);
	worker_finalize();
	return 0;
}
