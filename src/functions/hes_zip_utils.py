from datetime import datetime
from zipfile import ZipFile

DEFAULT_BATCH_SIZE = 10000
DEFAULT_LIMIT = float('inf')
SEPARATOR = '|'

def get_filename(zip_path):
    with ZipFile(zip_path) as zipf:
        return zipf.filelist[0].filename


# Return generator of rows, where each row is a list of string elements
def get_zip_data_generator(path):
    filename = get_filename(path)

    # lines = ZipFile(path).open(filename).readlines()
    # print(lines)
    #
    # lines = ZipFile(path).open(filename).readlines(2)
    # print(lines)
    #
    # lines = ZipFile(path).open(filename).readline(10000)
    # print(lines)

    file = ZipFile(path).open(filename)
    return (line.strip().decode("utf-8") for line in file)


def get_header_from_zip(path):
    return next(get_zip_data_generator(path))


def batch_datalines_and_process(data_lines_gen: list, limit, batch_size, func):
  limit = limit or DEFAULT_LIMIT
  batch_size = batch_size or DEFAULT_BATCH_SIZE
  print(f"batching rows with limit of {limit} and batch size {batch_size} ")

  totali=1
  is_not_finished=True
  while is_not_finished:
      lines_batch=[]
      while is_not_finished and (len(lines_batch) < batch_size):
          line = next(data_lines_gen)
          print(f"{totali}: {line}")
          if (line is None or line == "") or (len(line) < 5):
              print(f"Reached final line: {line}")
              is_not_finished=False
          else:
              if totali >= limit:
                  print(f"Reached limit: {limit}")
                  is_not_finished = False

              lines_batch.append(line)
              if is_not_finished:
                totali += 1


      print(f"Finished reading batch of {len(lines_batch)} lines")
      func(lines_batch)
      currTime = datetime.now().strftime("%H:%M:%S")
      print(f"Completed processing batch. Total count now {totali}. Time is {currTime}")
      print(f"Processed {totali} lines")

  print(f"finished batching {totali} rows with limit of {limit} and batch size {batch_size} ")
  print("COMPLETED READING")