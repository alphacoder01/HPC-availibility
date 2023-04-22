# Usage:
- `bash check_free_nodes.sh`
- The following content will be printed:
    - for all nodes in HPC if resource available:
        - `Node name`:
            - `Num. CPU cores available`
            - `Num. GPU available`
    - For AMD and Icelake Nodes:
        - Top 10 jobs will be listed sorted by end-time in ascending order with following keys
        - `Job ID -- Node Name -- Owner -- Job Start Time -- Total Requested Time -- Resources Allocated -- End Timestamp`