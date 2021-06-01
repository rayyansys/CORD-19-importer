#!/usr/bin/env python3
# Changes based on 202003207 release
# Need to write something here ...
# Changes based on 202003207 release
# Change has_full_text --> has_pdf_parse
# Add has_pmc_xml_parse and cord_uid to notes

USAGE = '''\
Converts open COVID-19 dataset to rayyan compatible form
usage: python transform_to_rayyan.py <input_file.csv> <output_file.csv>\
'''

import csv
import sys
import ast
import numpy

import multiprocessing as mp

from dateparser import parse as normalparse
from daterangeparser import parse as rangeparse

from tqdm import tqdm

if len(sys.argv) != 3:
  print(USAGE)
  print("Incorrect number of arguments!")
  exit(1)

#DX_DOI_PREFIX = 'http://dx.doi.org/'

def transform_row_to_rayyan(irow):
    orow = {}

    orow['title'] = irow['title']
    orow['abstract'] = irow['abstract']
    #orow['url'] = DX_DOI_PREFIX + irow['doi']
    orow['url'] = irow['url']
    orow['pmc_id'] = irow['pmcid']
    orow['pubmed_id'] = irow['pubmed_id']

    publish_time = irow['publish_time'].strip()
    try:
      # First, try parsing as a daterange.
      # This should catch most date formats except
      # those in 'DD-MM-YY' and some other forms.
      start, end = rangeparse(publish_time)
    except:
      # If parsing as daterange fails, select
      # the first word. It's usually 'YYYY' or 'DD-MM-YY'
      # which is good enough.
      publish_time = publish_time.split(' ')[0]

    if publish_time:
      try:
        # Try another parse as daterange
        start, end = rangeparse(publish_time)
      except:
        # If that fails, then it is ''DD-MM-YY',
        # which can be picked up by normalparse.
        start = normalparse(publish_time)

      orow['year'] = start.year
      orow['month'] = start.month
      orow['day'] = start.day
    else:
      orow['year'] = ''
      orow['month'] = ''
      orow['day'] = ''

    # Inital dataset had authors in a list form.
    # Try parsing authors to see if it's a list.
    try:
      authors = ast.literal_eval(irow['authors'])
      if type(authors) == list:
        orow['authors'] = '; '.join(authors)
      else:
        raise RuntimeError
    except:
      # It's not a list, use the string as is.
      orow['authors'] = irow['authors']

    orow['journal'] = irow['journal']

    notes = []
    for col in ['cord_uid', 'sha', 'doi', 'source_x', 'license', 'mag_id', 'who_covidence_id', 'arxiv_id', 's2_id', 'pdf_json_files', 'pmc_json_files']:
      notes.append(col + ': ' + irow[col])

    orow['notes'] = '; '.join(notes)
    return orow

def batch_tranform_to_rayyan(process_number, rows):
  pbar = tqdm(desc="Process # %s" % process_number, total=len(rows), position=process_number)
  output = []

  for r in rows:
    output.append(transform_row_to_rayyan(r))
    pbar.update()

  pbar.close()
  return output

NUM_CORES = 8

OUTPUT_FIELDS = ['title', 'abstract', 'url', 'pmc_id', 'pubmed_id', 'year', 'month', 'day', 'authors', 'journal', 'notes']

if __name__ == "__main__":
  input_csv = csv.DictReader(open(sys.argv[1], 'r', encoding='utf-8', errors='ignore'), delimiter=',')
  output_csv = csv.DictWriter(open(sys.argv[2], "w+"), delimiter=',', fieldnames=OUTPUT_FIELDS)

  output_csv.writerow(dict((fn, fn) for fn in OUTPUT_FIELDS))

  # Gather all rows into memory
  all_input_rows = [input_row for input_row in input_csv]

  # Split rows into NUM_CORES chunks for parallel processing
  input_chunks = numpy.array_split(numpy.array(all_input_rows), NUM_CORES)
  input_chunks_with_index = [(index, chunk) for index, chunk in enumerate(input_chunks)]

  # Create pool of workers
  pool = mp.Pool(initializer=tqdm.set_lock, initargs=(tqdm.get_lock(),), processes=NUM_CORES)

  # Apply transformation in parallel
  print("Starting transformation with %s workers..." % NUM_CORES)
  output_chunks = pool.starmap(batch_tranform_to_rayyan, input_chunks_with_index)

  # Wrap up workers
  pool.close()
  pool.join()

  # Write output to file
  for chunk in output_chunks:
    for row in chunk:
      output_csv.writerow(row)

  print("Complete.")
