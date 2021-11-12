#ifndef REFORMAT_H_
#define REFORMAT_H_

//edge file -> vertex file with adj-lists
//append values, seperated by " ", for each key

#include "ydhdfs.h"
#include "global.h"
#include "serialization.h"
#include "communication.h"
#include "time.h"
#include <vector>
#include <iostream>

//============= to allow string to be ID =============

namespace __gnu_cxx {
	template <>
	struct hash<string> {
		size_t operator()(string str) const
		{
			return __stl_hash_string(str.c_str());
		}
	};
}

class StringHash {
	public:
		inline int operator()(string str)
		{
			return __gnu_cxx::__stl_hash_string(str.c_str()) % ((unsigned int)_num_workers);
		}
};
//============= to allow string to be ID =============

template <class T>
struct KeyString {
    T key;
    string content;
};

template <class T>
ibinstream& operator<<(ibinstream& m, const KeyString<T>& v)
{
    m << v.key;
    m << v.content;
    return m;
}

template <class T>
obinstream& operator>>(obinstream& m, KeyString<T>& v)
{
    m >> v.key;
    m >> v.content;
    return m;
}

template <class T>
class FormatWorker {
	StringHash hash;
public:
    typedef KeyString<T> Item;
    typedef vector<Item*> ItemContainer;

    ItemContainer items;

    ~FormatWorker()
    {
        for (int i = 0; i < items.size(); i++)
            delete items[i];
    }
    
    //user-defined graphLoader ==============================
    virtual Item* to_keystr(char* line) = 0; //this is what user specifies!!!!!!

    void load_graph(const char* inpath)
    {
        hdfsFS fs = getHdfsFS();
        hdfsFile in = getRHandle(inpath, fs);
        LineReader reader(fs, in);
        while (true) {
            reader.readLine();
            if (!reader.eof())
            	items.push_back(to_keystr(reader.getLine()));
            else
                break;
        }
        hdfsCloseFile(fs, in);
        hdfsDisconnect(fs);
        //cout<<"Worker "<<_my_rank<<": \""<<inpath<<"\" loaded"<<endl;//DEBUG !!!!!!!!!!
    }
    //=======================================================

    //user-defined graphDumper ==============================
    virtual void to_line(Item* v) = 0; //this is what user specifies!!!!!!

    vector<char> buf;
    void write(const char* content)
    {
        int len = strlen(content);
        buf.insert(buf.end(), content, content + len);
    }

    void dump_partition(const char* outpath)
    {
        hdfsFS fs = getHdfsFS();
        char tmp[5];
        sprintf(tmp, "%d", _my_rank);
        hdfsFile hdl = getWHandle((string(outpath) + "/part_" + string(tmp)).c_str(), fs);
        for (int i = 0; i < items.size(); i++) {
            Item* v = items[i];
            if (buf.size() >= HDFS_BLOCK_SIZE) {
                tSize numWritten = hdfsWrite(fs, hdl, &buf[0], buf.size());
                if (numWritten == -1) {
                    fprintf(stderr, "Failed to write file!\n");
                    exit(-1);
                }
                buf.clear();
            }
            to_line(v);
        }
        tSize numWritten = hdfsWrite(fs, hdl, &buf[0], buf.size());
        if (numWritten == -1) {
            fprintf(stderr, "Failed to write file!\n");
            exit(-1);
        }
        buf.clear();
        if (hdfsFlush(fs, hdl)) {
            fprintf(stderr, "Failed to flush");
            exit(-1);
        }
        hdfsCloseFile(fs, hdl);
        hdfsDisconnect(fs);
    }
    //=======================================================

    //step 1: hash items to dest-worker
    void sync_to_workers()
	{
		ResetTimer(4);
		//set send buffer
		vector<ItemContainer> _loaded_parts(_num_workers);
		for (int i = 0; i < items.size(); i++) {
			Item* v = items[i];
			_loaded_parts[hash(v->key)].push_back(v);
		}
		//exchange items to add
		all_to_all(_loaded_parts);
		//delete sent items
		for (int i = 0; i < items.size(); i++) {
			Item* v = items[i];
			if (hash(v->key) != _my_rank)
				delete v;
		}
		items.clear();
		//collect items to add
		for (int i = 0; i < _num_workers; i++) {
			items.insert(items.end(), _loaded_parts[i].begin(), _loaded_parts[i].end());
			
		}
		StopTimer(4);
		PrintTimer("Shuffle Time", 4);
	};
    
    //user-defined reducer ==============================
    //how to combine array[0..size-1]
    virtual Item* reduce(Item** array, int size) = 0; //this is what user specifies!!!!!!
    
    static bool PointerComp(const Item* lhs, const Item* rhs)
	{
		return lhs->key < rhs->key;
	};
    
    //step 2: combine items with the same key
	void sort_and_combine()
	{
		if(items.empty()) return;
		ResetTimer(4);
		//sort assigned items
		sort(items.begin(), items.end(), PointerComp);
		ItemContainer tmp;
		//get groups
		Item ** array = & items[0];
		int size = 1;
		int pos = 1;
		while(pos < items.size())
		{
			Item ** cur = array + size;
			if((*cur)->key != (*array)->key)
			{
				Item * output = reduce(array, size);
				tmp.push_back(output);
				array = cur;
				size = 1;
			}
			else size ++;
			pos ++;
		}
		Item * output = reduce(array, size);//flush last group
		tmp.push_back(output);
		//------
		for (int i = 0; i < items.size(); i++) delete items[i];
		items.swap(tmp);
		//------
		StopTimer(4);
		PrintTimer("Reduce Time", 4);
	}

    // run the worker
    void run(const WorkerParams& params)
    {
        //check path + init
        if (_my_rank == MASTER_RANK) {
            if (dirCheck(params.input_path.c_str(), params.output_path.c_str(), _my_rank == MASTER_RANK, params.force_write) == -1)
                exit(-1);
        }
        init_timers();

        //dispatch splits
        ResetTimer(WORKER_TIMER);
        vector<vector<string> >* arrangement;
        if (_my_rank == MASTER_RANK) {
            arrangement = params.native_dispatcher ? dispatchLocality(params.input_path.c_str()) : dispatchRan(params.input_path.c_str());
            //reportAssignment(arrangement);//DEBUG !!!!!!!!!!
            masterScatter(*arrangement);
            vector<string>& assignedSplits = (*arrangement)[0];
            //reading assigned splits (map)
            for (vector<string>::iterator it = assignedSplits.begin();
                 it != assignedSplits.end(); it++)
                load_graph(it->c_str());
            delete arrangement;
        } else {
            vector<string> assignedSplits;
            slaveScatter(assignedSplits);
            //reading assigned splits (map)
            for (vector<string>::iterator it = assignedSplits.begin();
                 it != assignedSplits.end(); it++)
                load_graph(it->c_str());
        }
        StopTimer(WORKER_TIMER);
        PrintTimer("Load Time", WORKER_TIMER);

        //=========================================================

        init_timers();
        ResetTimer(WORKER_TIMER);
        sync_to_workers();
        sort_and_combine();
        StopTimer(WORKER_TIMER);
        PrintTimer("Communication Time", COMMUNICATION_TIMER);
        PrintTimer("- Serialization Time", SERIALIZATION_TIMER);
        PrintTimer("- Transfer Time", TRANSFER_TIMER);
        PrintTimer("Total Computational Time", WORKER_TIMER);
        
        // dump graph
        ResetTimer(WORKER_TIMER);
        dump_partition(params.output_path.c_str());
        worker_barrier();
        StopTimer(WORKER_TIMER);
        PrintTimer("Dump Time", WORKER_TIMER);
    }
};

#endif
