
import argparse
import os.path

BUFSIZE_KB = 1024

parser = argparse.ArgumentParser()
parser.add_argument('step', type=int)
parser.add_argument('size_kb', type=int)
parser.add_argument('output_dir', type=str)

args = parser.parse_args()
outfile = os.path.join(args.output_dir, "step_{}".format(args.step))

buf = "a" * (BUFSIZE_KB * 1024)

with open(outfile, "w") as f:
    nwrites, rem = divmod(args.size_kb, BUFSIZE_KB)
    for _ in range(nwrites):
        f.write(buf)
    if rem > 0:
        f.write(buf[:rem * 1024])

