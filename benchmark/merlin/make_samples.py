import argparse

# argument parsing
parser = argparse.ArgumentParser(description="Make some samples.")
parser.add_argument(
    "--number", type=int, action="store", help="the number of samples you want to make"
)
parser.add_argument("--filepath", type=str, help="output file")
args = parser.parse_args()

results = [(str(i) + '\n') for i in range(args.number)]
results = ''.join(results)

with open(args.filepath, "w") as f:
    f.write(results)
