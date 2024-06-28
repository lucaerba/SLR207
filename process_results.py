import re
import matplotlib.pyplot as plt

def parse_parallel_results(content):
    result = []
    blocks = content.split("----")
    for block in blocks:
        if block.strip() == "":
            continue
        data = {}
        lines = block.strip().split('\n')
        for line in lines:
            key_value = line.split(": ")
            if len(key_value) == 2:
                key, value = key_value
                key = key.strip()
                value = value.strip()
                if key in ['n_server', 'n_files', 'dim']:
                    data[key] = int(value)
                else:
                    data[key] = float(value)
        if data:
            result.append(data)
    return result

def parse_simple_results(content):
    result = []
    lines = content.split('\n')
    data = {}
    for line in lines:
        key_value = line.split(": ")
        if len(key_value) == 2:
            key, value = key_value
            key = key.strip()
            value = value.strip()
            if key == 'n_files':
                if data:
                    result.append(data)
                data = {'n_files': int(value)}
            elif key == 'total time':
                data['total time'] = float(value)
    if data:
        result.append(data)
    return result

def plot_graphs(parallel_results, simple_results):
    # Fixed number of machines vs simple in increasing size
    fixed_machines = 6
    parallel_times = [res['total time'] for res in parallel_results if res['n_server'] == fixed_machines]
    simple_times = [res['total time'] for res in simple_results if res['n_files'] <= len(parallel_times)]
    file_counts = [res['n_files'] for res in parallel_results if res['n_server'] == fixed_machines]
    
    plt.figure(figsize=(10, 5))
    plt.plot(file_counts, parallel_times, label=f'Parallel ({fixed_machines} machines)')
    plt.plot(file_counts[:len(simple_times)], simple_times, label='Simple')
    plt.xlabel('Number of Files')
    plt.ylabel('Total Time (ms)')
    plt.title(f'Performance: Fixed {fixed_machines} Machines vs Simple in Increasing Size')
    plt.legend()
    plt.savefig('fixed_machines_vs_simple.png')
    plt.show()

    # Fixed number of files, to see the speed up
    fixed_files = 3
    server_counts = sorted(set(res['n_server'] for res in parallel_results if res['n_files'] == fixed_files))
    parallel_times = [res['total time'] for res in parallel_results if res['n_files'] == fixed_files]
    
    plt.figure(figsize=(10, 5))
    plt.plot(server_counts, parallel_times, label=f'Parallel with {fixed_files} files')
    plt.xlabel('Number of Machines')
    plt.ylabel('Total Time (ms)')
    plt.title(f'Performance: Fixed {fixed_files} Files vs Number of Machines')
    plt.legend()
    plt.savefig('fixed_files_vs_machines.png')
    plt.show()

def main():
    # Read from files
    with open('res', 'r') as f:
        parallel_content = f.read()
    with open('res_simple', 'r') as f:
        simple_content = f.read()
    
    # Parse results
    parallel_results = parse_parallel_results(parallel_content)
    simple_results = parse_simple_results(simple_content)
    
    # Print results
    print("Parallel Results:")
    for result in parallel_results:
        print(result)
    
    print("\nSimple Results:")
    for result in simple_results:
        print(result)
    
    # Plot graphs
    plot_graphs(parallel_results, simple_results)

if __name__ == "__main__":
    main()
