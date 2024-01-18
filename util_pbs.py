import json
import sys
import os
import pandas as pd

if __name__ == '__main__':
    with open('./test_pbs.json', 'r') as f:
        x = json.load(f)

    for node in x['nodes'].keys():
        detail = x['nodes'][node]
        if 'ngpus' in detail['resources_available'] and detail['state']=='free':
            gpu_on_node = detail['resources_available']['ngpus']
            try:
                gpu_alloted = detail['resources_assigned']['ngpus']
            except:
                continue

            if gpu_on_node - gpu_alloted > 0:
                # print(detail)
                print("Node: ",detail['Mom'])
                print("CPU available: ",detail['resources_available']['ncpus'] - detail['resources_assigned']['ncpus'])
                print("GPU available: ",detail['resources_available']['ngpus'] - detail['resources_assigned']['ngpus'])
                print('\n')

    # AMD nodes info
    print('ScAI NODES:')
    node_name =[]
    owners = []
    sTime = []
    eTime = []
    R = []
    job_name = []
    for node in x['nodes'].keys():
        detail = x['nodes'][node]
        if 'scai' in detail['Mom'] and (detail['state'] not  in ['down,offline','offline']):
            try:
                jobs = detail['jobs']
            except:
                continue
            for job in jobs:
                node_name.append(detail['Mom'].split('.')[0])
                os.system(f"qstat -f {job} -F json >> amd.json")
                with open('./amd.json', 'r') as f:
                    A = json.load(f)
                job_name.append(job)
                owners.append(A['Jobs'][job]['Job_Owner'].split('@')[0])
                sTime.append(A['Jobs'][job]['stime'])
                eTime.append(A['Jobs'][job]['Resource_List']['walltime'])
                R.append((A['Jobs'][job]['Resource_List']['ncpus'],A['Jobs'][job]['Resource_List']['ngpus']))
                os.system("rm -rf ./amd.json")
    
    df = pd.DataFrame(list(zip(job_name,node_name,owners,sTime, eTime, R)), columns = ['Job ID','Node','Owner','StartTime','EndTime','Resources (cpu, gpu)'])
    df['endDate'] = pd.to_datetime(df['StartTime'])+pd.to_timedelta(df['EndTime'])
    sorted_df = df.sort_values('endDate')
    sorted_df = sorted_df.drop_duplicates('Job ID')
    sorted_df.reset_index(drop = True, inplace = True)
    print(sorted_df[:10])

    U2G = {}
    for k in range(len(sorted_df)):
        C,G = sorted_df['Resources (cpu, gpu)'].iloc[k]
        usr = sorted_df['Owner'].iloc[k]
        if G > 0:
            if usr not in U2G:
                U2G[usr]=1
            else:
                U2G[usr] +=1
    print(U2G)
            



    #IceLake Nodes
    print('Icelake NODES:')
    node_name =[]
    owners = []
    sTime = []
    eTime = []
    R = []
    job_name = []
    for node in x['nodes'].keys():
        detail = x['nodes'][node]
        try:
            if 'aice' in detail['Mom'] and (detail['state'] not  in ['down,offline','offline']):
                jobs = detail['jobs']
                for job in jobs:
                    os.system(f"qstat -f {job} -F json >> ice.json")
                    with open('./ice.json', 'r') as f:
                        A = json.load(f)
                    try:
                        TT = (A['Jobs'][job]['Resource_List']['ngpus'])
                    except:
                        os.system("rm -rf ./ice.json")
                        continue
                    
                    node_name.append(detail['Mom'].split('.')[0])
                    job_name.append(job)
                    owners.append(A['Jobs'][job]['Job_Owner'].split('@')[0])
                    sTime.append(A['Jobs'][job]['stime'])
                    eTime.append(A['Jobs'][job]['Resource_List']['walltime'])
                    R.append((A['Jobs'][job]['Resource_List']['ncpus'],A['Jobs'][job]['Resource_List']['ngpus']))
                    os.system("rm -rf ./ice.json")
        except:
            continue
        
    df = pd.DataFrame(list(zip(job_name,node_name,owners,sTime, eTime, R)), columns = ['Job ID','Node','Owner','StartTime','EndTime','Resources (cpu, gpu)'])
    df['endDate'] = pd.to_datetime(df['StartTime'])+pd.to_timedelta(df['EndTime'])
    sorted_df = df.sort_values('endDate')
    sorted_df = sorted_df.drop_duplicates('Job ID')
    sorted_df.reset_index(drop = True, inplace = True)
    print(sorted_df[:10])


    U2G = {}
    for k in range(len(sorted_df)):
        C,G = sorted_df['Resources (cpu, gpu)'].iloc[k]
        usr = sorted_df['Owner'].iloc[k]
        if G > 0:
            if usr not in U2G:
                U2G[usr]=1
            else:
                U2G[usr] +=1
    print(U2G)




                