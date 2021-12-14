//########################################################################
//## Copyright 2018 Da Yan http://www.cs.uab.edu/yanda
//##
//## Licensed under the Apache License, Version 2.0 (the "License");
//## you may not use this file except in compliance with the License.
//## You may obtain a copy of the License at
//##
//## //http://www.apache.org/licenses/LICENSE-2.0
//##
//## Unless required by applicable law or agreed to in writing, software
//## distributed under the License is distributed on an "AS IS" BASIS,
//## WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//## See the License for the specific language governing permissions and
//## limitations under the License.
//########################################################################

#include "subg-dev.h"

//note for changing to triangle enumeration
//for triangle v1-v2-v3 with v1<v2<v3, we can maintain v1 in task.context if we output triangles

typedef char Label;

struct AdjItem
{
    AdjItem(VertexID id_ = 0):id(id_) {}
	VertexID id;
};

struct TriangleTriangleValue
{
	vector<AdjItem> adj;
};

typedef Vertex<VertexID, TriangleTriangleValue> TriangleTriangleVertex;
typedef Subgraph<TriangleTriangleVertex> TriangleTriangleSubgraph;
typedef Task<TriangleTriangleVertex, char> TriangleTriangleTask; //context = step

obinstream & operator>>(obinstream & m, AdjItem & v)
{
    m >> v.id;
    return m;
}

ibinstream & operator<<(ibinstream & m, const AdjItem & v)
{
    m << v.id;
    return m;
}

ofbinstream & operator>>(ofbinstream & m, AdjItem & v)
{
    m >> v.id;
    return m;
}

ifbinstream & operator<<(ifbinstream & m, const AdjItem & v)
{
    m << v.id;
    return m;
}

//------------------
obinstream & operator>>(obinstream & m, TriangleTriangleValue & Val)
{
    m >> Val.adj;
    return m;
}

ibinstream & operator<<(ibinstream & m, const TriangleTriangleValue & Val)
{
    m << Val.adj;
    return m;
}

ofbinstream & operator>>(ofbinstream & m, TriangleTriangleValue & Val)
{
    m >> Val.adj;
    return m;
}

ifbinstream & operator<<(ifbinstream & m, const TriangleTriangleValue & Val)
{
    m << Val.adj;
    return m;
}
//-------------------
// add a node to graph: only id and label of v, not its edges
// must make sure g.hasVertex(v.id) == true !!!!!!
void addNode(TriangleTriangleSubgraph & g, TriangleTriangleVertex & v)
{
	TriangleTriangleVertex temp_v;
	temp_v.id = v.id;
	g.addVertex(temp_v);
}

void addNode_safe(TriangleTriangleSubgraph & g, VertexID id) // avoid redundancy
{
    if (g.hasVertex(id))
        return ;
	TriangleTriangleVertex temp_v;
	temp_v.id = id;
	g.addVertex(temp_v);
}

// add a edge to graph
// must make sure id1 and id2 are added first !!!!!!
void addEdge(TriangleTriangleSubgraph & g, VertexID id1, VertexID id2)
{
    TriangleTriangleVertex * v1, * v2;
    v1 = g.getVertex(id1);
    v2 = g.getVertex(id2);
    AdjItem temp_adj;
	temp_adj.id = v2->id;
	v1->value.adj.push_back(temp_adj);
	temp_adj.id = v1->id;
	v2->value.adj.push_back(temp_adj);
}

void addEdge_safe(TriangleTriangleSubgraph & g, VertexID id1, VertexID id2) //avoid redundancy
{
    TriangleTriangleVertex * v1, * v2;
    v1 = g.getVertex(id1);
    v2 = g.getVertex(id2);
    int i = 0;
    vector<AdjItem> & adj = v2->value.adj;
    for(; i<adj.size(); i++)
    	if(adj[i].id == id1) break;
    if(i == adj.size())
    {
    	AdjItem temp_adj;
		temp_adj.id = v2->id;
		v1->value.adj.push_back(temp_adj);
		temp_adj.id = v1->id;
		v2->value.adj.push_back(temp_adj); // bigraph
    }
}

struct less_than_key
{
    inline bool operator() (const AdjItem& struct1, const AdjItem& struct2)
    {
        return (struct1.id < struct2.id);
    }
};

class TriangleTriangleTrimmer:public Trimmer<TriangleTriangleVertex>
{
    virtual void trim(TriangleTriangleVertex & v) {
    	TriangleTriangleValue & val = v.value;
    	/*
        TriangleTriangleValue newval;
        newval.l = 'z';
        for (int i = 0; i < val.adj.size(); i++) {
            if (v.id < val.adj[i].id)
            	newval.adj.push_back(val.adj[i]);
        }
        val.adj.swap(newval.adj);
        */
        sort(val.adj.begin(), val.adj.end(), less_than_key());
    }
};

class TriangleTriangleAgg:public Aggregator<unsigned long long, unsigned long long, unsigned long long>  //all args are counts
{
private:
	unsigned long long count;
	unsigned long long sum;

public:

    virtual void init()
    {
    	sum = count = 0;
    }

    virtual void init_udf(unsigned long long & prev) {
    	sum = 0;
    }

    virtual void aggregate_udf(unsigned long long & task_count)
    {
    	count += task_count;
    }

    virtual void stepFinal_udf(unsigned long long & partial_count)
    {
    	sum += partial_count; //add all other machines' counts (not master's)
    }

    virtual void finishPartial_udf(unsigned long long & collector)
    {
    	collector = count;
    }

    virtual void finishFinal_udf(unsigned long long & collector)
    {
    	sum += count; //add master itself's count
    	if(_my_rank == MASTER_RANK) cout<<"2-Star_1-Star Count = "<<sum<<endl;
    	collector = sum;
    }
};

class TriangleTriangleComper:public Comper<TriangleTriangleTask, TriangleTriangleAgg>
{
public:

	//check whether task is bigtask
	virtual bool is_bigtask(TriangleTriangleTask * task){
		if(task->subG.vertexes.size() > BIGTASK_THRESHOLD
					|| task->to_pull.size() > BIGTASK_THRESHOLD)
			return true;
		else
			return false;
	}

    virtual bool task_spawn(VertexT * v)
    {
    	if(v->value.adj.size() < 2) return false;
    	// cout<<v->id<<": in task_spawn"<<endl;//@@@@@@@@@@@@@
    	TriangleTriangleTask * t = new TriangleTriangleTask;
        addNode_safe(t->subG, v->id); // a
        for(int i=0; i<v->value.adj.size(); i++) //-1 since we do not need to pull the largest vertex
        {
            addNode_safe(t->subG, v->value.adj[i].id);
            addEdge_safe(t->subG, v->value.adj[i].id, v->id);
            VertexID nb = v->value.adj[i].id;
            t->pull(nb);
        }
        t->context = 1;
        bool result = is_bigtask(t);
        add_task(t);
        return result;
    } 

    void triangle_count(SubgraphT & g, vector<VertexT *> & frontier, unsigned long long & count)
	{
        TriangleTriangleVertex & root = g.vertexes[0];
        unsigned long long other_nodes = 0;
        vector<VertexID> GMatchQ;
        GMatchQ.push_back(root.id);
        for (int j = 0; j < root.value.adj.size(); j++)
        {
            VertexT * u = g.getVertex(root.value.adj[j].id);
            GMatchQ.push_back(root.value.adj[j].id);
            for (int l = 0; l < u->value.adj.size(); l++)
            {
                if (find(GMatchQ.begin(), GMatchQ.end(), u->value.adj[l].id) != GMatchQ.end()) continue;
                GMatchQ.push_back(u->value.adj[l].id);
                other_nodes = 0;
                for (int k = 0; k < root.value.adj.size(); k++)
                {
                    if (find(GMatchQ.begin(), GMatchQ.end(), root.value.adj[k].id) != GMatchQ.end()) continue;
                    other_nodes++;
                }
                count += (other_nodes - 1) * other_nodes / 2;
                GMatchQ.pop_back();
            }
            GMatchQ.pop_back();
        }
        GMatchQ.pop_back();
	}

    virtual bool compute(SubgraphT & g, ContextT & context, vector<VertexT *> & frontier)
    {
        if (context <= 1)
        {
            for (int i = 0; i < frontier.size(); i++)
            {
                for (int j = 0; j < frontier[i]->value.adj.size(); j++)
                {
                    addNode_safe(g, frontier[i]->value.adj[j].id);
                    addEdge_safe(g, frontier[i]->value.adj[j].id, frontier[i]->id);
                    pull(frontier[i]->value.adj[j].id);
                }
            }
            context++;
            return true;
        }
        else
        {
            //run single-threaded mining code
            unsigned long long count = 0;
            triangle_count(g, frontier, count);
            TriangleTriangleAgg* agg = get_aggregator();
            agg->aggregate(count);
            //cout<<rootID<<": done"<<endl;//@@@@@@@@@@@@@
            return false;
        }
    }
};

class TriangleTriangleWorker:public Worker<TriangleTriangleComper>
{
public:
	TriangleTriangleWorker(int num_compers) : Worker<TriangleTriangleComper>(num_compers){}

    virtual VertexT* toVertex(char* line)
    {
        VertexT* v = new VertexT;
        char * pch;
        pch=strtok(line, " \t");
        v->id=atoi(pch);
        strtok(NULL," \t");
        TriangleTriangleValue & val = v->value;
        while((pch=strtok(NULL, " ")) != NULL)
        {
            val.adj.push_back(atoi(pch));
        }
        return v;
    }

    virtual void task_spawn(VertexT * v, vector<TriangleTriangleTask*> & tcollector)
	{
    	if(v->value.adj.size() < 2) return;
    	TriangleTriangleTask* task = new TriangleTriangleTask;
		addNode_safe(task->subG, v->id); // a
        for(int i=0; i<v->value.adj.size(); i++) //-1 since we do not need to pull the largest vertex
        {
            addNode_safe(task->subG, v->value.adj[i].id);
            addEdge_safe(task->subG, v->value.adj[i].id, v->id);
            VertexID nb = v->value.adj[i].id;
            task->pull(nb);
        }
        task->context = 1;
		tcollector.push_back(task);
    }
};

int main(int argc, char* argv[])
{
    init_worker(&argc, &argv);
    WorkerParams param;
    if(argc != 4){
		cout<<"arg1 = input path in HDFS, arg2 = number of threads, arg3 = BIGTASK_THRESHOLD"<<endl;
		return -1;
	}
    param.input_path = argv[1];  //input path in HDFS
    int thread_num = atoi(argv[2]);  //number of threads per process
    BIGTASK_THRESHOLD = atoi(argv[3]);
    param.force_write=true;
    param.native_dispatcher=false;
    //------
    TriangleTriangleTrimmer trimmer;
    TriangleTriangleAgg aggregator;
    TriangleTriangleWorker worker(thread_num);
    worker.setTrimmer(&trimmer);
    worker.setAggregator(&aggregator);
    auto start = chrono::high_resolution_clock::now();
    worker.run(param);
    worker_finalize();
    auto stop = chrono::high_resolution_clock::now();
    auto duration = chrono::duration_cast<chrono::microseconds>(stop - start);
	cout << "running time(us) = " << duration.count() << endl;
    return 0;
}
