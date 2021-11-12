#ifndef EDGEFORMAT_H_
#define EDGEFORMAT_H_

//edge file -> vertex file with adj-lists
//output: srcID srcAttrStr \t neighborlist

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
struct Edge {
    T src;
    T dst;
};

template <class T>
ibinstream& operator<<(ibinstream& m, const Edge<T>& v)
{
    m << v.src;
    m << v.dst;
    return m;
}

template <class T>
obinstream& operator>>(obinstream& m, Edge<T>& v)
{
    m >> v.src;
    m >> v.dst;
    return m;
}

//====================================================

template <class T>
struct Adjlist {
    T src;
    vector<T> dst;
};

template <class T>
ibinstream& operator<<(ibinstream& m, const Adjlist<T>& v)
{
    m << v.src;
    m << v.dst;
    return m;
}

template <class T>
obinstream& operator>>(obinstream& m, Adjlist<T>& v)
{
    m >> v.src;
    m >> v.dst;
    return m;
}

//====================================================

template <class T = string>
class FormatWorker {
	StringHash hash;
public:
    typedef Edge<T> Item;
    typedef Adjlist<T> List;
    typedef vector<Item*> ItemContainer; //each item will be freed after used to construct lists
    typedef vector<List> ListContainer;

    ItemContainer items;
    ListContainer lists;
    
    //user-defined graphLoader ==============================
    virtual Item* to_edge(char* line) //this is what user specifies!!!!!!
    {
        //default implementation, only applicable for T = std::string
        char * pch;
        pch = strtok(line, " \t");
        Item* v = new Item;
        v->src = pch;
        pch = strtok(NULL, " \t");
        v->dst = pch;
        return v;
    }

    void load_graph(const char* inpath)
    {
        hdfsFS fs = getHdfsFS();
        hdfsFile in = getRHandle(inpath, fs);
        LineReader reader(fs, in);
        while (true) {
            reader.readLine();
            if (!reader.eof())
            	items.push_back(to_edge(reader.getLine()));
            else
                break;
        }
        hdfsCloseFile(fs, in);
        hdfsDisconnect(fs);
        //cout<<"Worker "<<_my_rank<<": \""<<inpath<<"\" loaded"<<endl;//DEBUG !!!!!!!!!!
    }
    //=======================================================

    //user-defined graphDumper ==============================
    virtual void to_line(List& list) //this is what user specifies!!!!!!
    {
        //default implementation, only applicable for T = std::string
        string tmp = list.src + '\t';
        vector<T> & dst = list.dst;
        int size = dst.size();
        if(size == 0) tmp += '\n';
        else
        {
            unsigned int i=0;
            for(; i<dst.size()-1; i++) tmp += dst[i] + " ";
            tmp += dst[i] + '\n';
        }
        write(tmp.c_str());
    }

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
        for (int i = 0; i < lists.size(); i++) {
            List & list = lists[i];
            if (buf.size() >= HDFS_BLOCK_SIZE) {
                tSize numWritten = hdfsWrite(fs, hdl, &buf[0], buf.size());
                if (numWritten == -1) {
                    fprintf(stderr, "Failed to write file!\n");
                    exit(-1);
                }
                buf.clear();
            }
            to_line(list);
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
			_loaded_parts[hash(v->src)].push_back(v);
		}
		//exchange items to add
		all_to_all(_loaded_parts);
		//delete sent items
		for (int i = 0; i < items.size(); i++) {
			Item* v = items[i];
			if (hash(v->src) != _my_rank)
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
    
    //============================== merge dsts into dst-list
    void reduce(Item** array, int size)
    {
        cout<<"size = "<<size<<endl;//!!!!!!!!!!!!!
        lists.push_back(List());
        lists.back().src = (*array)->src;
        vector<T> & dsts = lists.back().dst;
        for(int i=0; i<size; i++)
        {
            dsts.push_back((*array)->dst);
            delete (*array); //free Edge right after used
            array++;
        }
    }
    
    static bool PointerComp(const Item* lhs, const Item* rhs)
	{
		return lhs->src < rhs->src;
	};
    
    //step 2: combine items with the same key
	void sort_and_combine()
	{
        if(items.empty()) return;
		ResetTimer(4);
		//sort assigned items
		sort(items.begin(), items.end(), PointerComp);
		//get groups
		Item ** array = & items[0];
		int size = 1;
		int pos = 1;
		while(pos < items.size())
		{
			Item ** cur = array + size;
			if((*cur)->src != (*array)->src)
			{
                reduce(array, size);
				array = cur;
				size = 1;
			}
			else size ++;
			pos ++;
		}
		reduce(array, size);//flush last group
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
