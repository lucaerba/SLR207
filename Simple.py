import os
import time

folder = "/cal/commoncrawl/"
output_file = "output_simple.txt"
time_file = "time_simple.txt"

def process_files(n_files):
    start = time.time()
    files = [f for f in os.listdir(folder) if f.endswith('.warc.wet')]
    len_files = len(files)
    files = files[:n_files]  # Limit to the first n_files
    print(f"Processing {n_files} files out of {len_files} total files")

    word_count_map = {}

    for file in files:
        try:
            with open(os.path.join(folder, file), "r") as f:
                print("Reading file: " + file)
                for line in f:
                    line.replace("\n", " ")
                    #trim the line
                    line = line.strip()
                    words = line.split(" ")
                    for word in words:
                        if word in word_count_map:
                            word_count_map[word] += 1
                        else:
                            word_count_map[word] = 1
                f.close()
        except Exception as e:
            print(f"Error processing file {file}: {e}")

    # Sort by increasing value and then alphabetically
    sorted_word_count_map = sorted(word_count_map.items(), key=lambda x: (x[1], x[0]))

    # Write the sorted map to a file
    with open(output_file, "w") as f:
        for key, value in sorted_word_count_map:
            f.write(key + " " + str(value) + "\n")

    end = time.time()

    # Write the time taken to a file
    with open(time_file, "a") as f:
        f.write(f"n_files: {n_files}\n")
        f.write(f"total time: {end - start}\n")

    print(f"Time taken for {n_files} files: {end - start} seconds")
    print(f"Total number of unique words: {len(sorted_word_count_map)}")

# Main execution
total_files = len([f for f in os.listdir(folder) if f.endswith('.warc.wet')])
increment = 1  # Number of files to process in each iteration

for n_files in range(increment, total_files + 1, increment):
    process_files(n_files)
