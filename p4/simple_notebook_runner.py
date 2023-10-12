import nbformat
from nbconvert.preprocessors import ExecutePreprocessor
from nbconvert.exporters import NotebookExporter
from traitlets.config import Config
from argparse import ArgumentParser
import json
import sys


mode  = sys.argv[1]

if int(mode) == 1:
    filename = 'notebooks/p4-part1.ipynb'
    with open(filename) as ff:
        nb_in = nbformat.read(ff, nbformat.NO_CONVERT)
        
    ep = ExecutePreprocessor(timeout=1000, kernel_name='python3')
    nb_out = ep.preprocess(nb_in)

    with open("part1-data.json", "w") as json_file:
        json.dump(nb_out, json_file)


if int(mode) == 2:
    filename = 'notebooks/p4-part2.ipynb'
    with open(filename) as ff:
        nb_in = nbformat.read(ff, nbformat.NO_CONVERT)
        
    ep = ExecutePreprocessor(timeout=1000, kernel_name='python3')
    nb_out = ep.preprocess(nb_in)

    with open("part2-data.json", "w") as json_file:
        json.dump(nb_out, json_file)
