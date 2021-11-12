#include "utils/EdgeFormat.h"
using namespace std;

void convert(string in_path, string out_path)
{
	WorkerParams param;
	param.input_path=in_path;
	param.output_path=out_path;
	param.force_write=true;
	param.native_dispatcher=false;
	FormatWorker<> worker;
	worker.run(param);
}
