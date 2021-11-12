#ifndef EDGEFORMAT_H_
#define EDGEFORMAT_H_

//edge file -> vertex file with adj-lists
//output: srcID \t neighborlist

#include "ydhdfs.h"
#include "global.h"
#include "serialization.h"
#include "communication.h"
#include "time.h"
#include <vector>
#include <iostream>
#include <fstream>

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

template <class T = string, class HashT = StringHash>
class FormatWorker {
	HashT hash;
public:
    typedef Edge<T> Item;
    typedef Adjlist<T> List;
    typedef vector<Item*> ItemContainer; //each item will be freed after used to construct lists
    typedef vector<List> ListContainer;
    typedef set<T> KeySet;

    ItemContainer items;
    ListContainer lists;
    set<T> ids;
    
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
    virtual void to_line(List& list, BufferedWriter& writer) //this is what user specifies!!!!!!
    {
        //default implementation, only applicable for T = std::string
        string & src = list.src;
        vector<T> & dst = list.dst;
        int size = dst.size();
        writer.write(src.c_str());
        char num[10];
        sprintf(num, " %d", size);
        writer.write(num);
        writer.write("\t");
        if(size == 0) writer.write("\n");
        else
        {
            unsigned int i=0;
            for(; i<dst.size()-1; i++){
                writer.write(dst[i].c_str());
                writer.write(" ");
            }
            writer.write(dst[i].c_str());
            writer.write("\n");
        }
    }

    void dump_partition(const char* outpath)
    {
        hdfsFS fs = getHdfsFS();
        BufferedWriter* writer = new BufferedWriter(outpath, fs, _my_rank);
        
        for(int i=0; i<lists.size(); i++) {
            writer->check();
            to_line(lists[i], *writer);
        }
        delete writer;
        hdfsDisconnect(fs);
    }
    //=======================================================

    //step 1: hash items to dest-worker
    void sync_to_workers()
	{
		ResetTimer(4);
		//set send buffer
		vector<ItemContainer> _loaded_parts(_num_workers);
        KeySet keyset; //for creating degree-0 vertices, only needs to obtain "dst" of (src, dst) since "src" will definitely generate a list %%%
		for (int i = 0; i < items.size(); i++) {
			Item* v = items[i];
            keyset.insert(v->dst);//collect dst-ID to keyset %%%
			_loaded_parts[hash(v->src)].push_back(v);
		}
        //%%% //%%% //%%% //%%% //%%% //%%% //%%% //%%% //%%% //%%% //%%%
        vector<vector<T> > _collected_IDs(_num_workers);
        for(typename KeySet::iterator it = keyset.begin(); it!=keyset.end(); it++)
        {
            const T & cur = *it;
            _collected_IDs[hash(cur)].push_back(cur);
        }
        keyset.clear(); //to make room for other memory usage
        //%%% //%%% //%%% //%%% //%%% //%%% //%%% //%%% //%%% //%%% //%%%
		//exchange items to add, and collected IDs
		all_to_all_cat(_loaded_parts, _collected_IDs); //%%%
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
            ids.insert(_collected_IDs[i].begin(), _collected_IDs[i].end());//gather collected IDs
		}
		StopTimer(4);
		PrintTimer("Shuffle Time", 4);
	};
    
    //============================== merge dsts into dst-list
    void reduce(Item** array, int size)
    {
        T src =(*array)->src;
        lists.push_back(List());
        lists.back().src = src;
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
        hash_set<T> curSet;//%%%
        for(int i=0; i<items.size(); i++) curSet.insert(items[i]->src);//%%%
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
		reduce(array, size); //flush last group
        //%%% //%%% //%%% //%%% //%%% //%%% //%%% //%%%
        for(typename set<T>::iterator it = ids.begin(); it != ids.end(); it++)
        {
            if(curSet.find(*it) == curSet.end())
            {
                lists.push_back(List());
                lists.back().src = *it;
            }
        }
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
