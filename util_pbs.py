import json
import sys
import os
import pandas as pd
from multiprocessing import Pool, cpu_count
from functools import partial
import uuid

def process_node(node, node_data, node_type):
    detail = node_data['nodes'][node]
    if node_type == 'amd' and 'scai' in detail['Mom']:
        prefix = 'amd'
    elif node_type == 'icelake' and 'aice' in detail['Mom']:
        prefix = 'ice'
    elif node_type == 'skylake' and 'vsky' in detail['Mom']:
        prefix = 'sky'
    else:
        return None

    if detail['state'] in ['down,offline', 'offline']:
        return None

    try:
        jobs = detail['jobs']
    except KeyError:
        return None

    results = []
    for job in jobs:
        # Generate a unique identifier for this file
        unique_id = str(uuid.uuid4())
        filename = f"{prefix}_{unique_id}.json"
        
        os.system(f"qstat -f {job} -F json >> {filename}")
        try:
            with open(filename, 'r') as f:
                A = json.load(f)
            
            try:
                gpu_count = A['Jobs'][job]['Resource_List']['ngpus']
            except KeyError:
                continue

            results.append({
                'Job ID': job,
                'Node': detail['Mom'].split('.')[0],
                'Owner': A['Jobs'][job]['Job_Owner'].split('@')[0],
                'StartTime': A['Jobs'][job]['stime'],
                'EndTime': A['Jobs'][job]['Resource_List']['walltime'],
                'Resources (cpu, gpu)': (A['Jobs'][job]['Resource_List']['ncpus'], gpu_count)
            })
        except json.JSONDecodeError:
            print(f"Error decoding JSON for job {job}")
        finally:
            # Always attempt to remove the file, even if an error occurred
            try:
                os.remove(filename)
            except OSError:
                print(f"Error removing file {filename}")
    
    return results

# The rest of the script remains the same

def process_node_type(node_type, data):
    with Pool(cpu_count()) as pool:
        results = pool.map(partial(process_node, node_data=data, node_type=node_type), data['nodes'].keys())
    
    results = [item for sublist in results if sublist is not None for item in sublist]
    
    df = pd.DataFrame(results)
    df['endDate'] = pd.to_datetime(df['StartTime']) + pd.to_timedelta(df['EndTime'])
    sorted_df = df.sort_values('endDate').drop_duplicates('Job ID').reset_index(drop=True)
    
    print(f'{node_type.upper()} NODES:')
    print(sorted_df[:10])

    U2G = {}
    for _, row in sorted_df.iterrows():
        _, G = row['Resources (cpu, gpu)']
        usr = row['Owner']
        if G > 0:
            U2G[usr] = U2G.get(usr, 0) + G
    print(U2G)

    return sorted_df

def print_available_resources(data):
    for node, detail in data['nodes'].items():
        if 'ngpus' in detail['resources_available'] and detail['state'] == 'free':
            gpu_on_node = detail['resources_available']['ngpus']
            gpu_alloted = detail['resources_assigned'].get('ngpus', 0)

            if gpu_on_node - gpu_alloted > 0:
                print("Node: ", detail['Mom'])
                if detail['resources_assigned'] == {}:
                    print("CPU available: ", detail['resources_available']['ncpus'])
                else:
                    print("CPU available: ", detail['resources_available']['ncpus'] - detail['resources_assigned']['ncpus'])
                print("GPU available: ", gpu_on_node - gpu_alloted)
                print('\n')

if __name__ == '__main__':
    with open('./test_pbs.json', 'r') as f:
        data = json.load(f)

    print_available_resources(data)

    node_types = ['amd', 'icelake', 'skylake']
    for node_type in node_types:
        process_node_type(node_type, data)
