# import unittest
# from pathlib import Path

# from functions.hes_zip_utils import get_zip_data_generator, batch_datalines_and_process


# class TestHesZipUtils(unittest.TestCase):
#     def test_load_first_lines(self):
#         input_path = f'{Path(__file__).resolve().parents[1]}/hes_input/test_ae.zip'
#         print(f"Loading from {input_path}")
#         line_generator = get_zip_data_generator(input_path)
#         header1 = next(line_generator)
#         first_row1 = next(line_generator)
#         print(header1)
#         print(first_row1)

#         # can get fresh generator
#         line_generator = get_zip_data_generator(input_path)
#         header2 = next(line_generator)
#         first_row2 = next(line_generator)
#         print(header2)
#         print(first_row2)
#         self.assertEqual(header1, header2)
#         self.assertEqual(first_row1, first_row2)

#     def test_batch_datalines_and_process(self):
#         input_path = f'{Path(__file__).resolve().parents[1]}/hes_input/test_ae.zip'
#         print(f"Loading from {input_path}")
#         line_generator = get_zip_data_generator(input_path)
#         next(line_generator)

#         d = {}
#         d['batch_count'] = 0
#         d['total_count'] = 0

#         def func(batch):
#             batch_count = d['batch_count'] + 1
#             batch_size = len(batch)
#             d['batch_count'] = batch_count
#             d['total_count'] = d['total_count'] + batch_size
#             print(f"batch_count:{batch_count} with size {batch_size}")

#         batch_datalines_and_process(line_generator, 3, 2, func)
#         self.assertEqual(2, d['batch_count'])
#         self.assertEqual(3, d['total_count'])

#         d['batch_count'] = 0
#         d['total_count'] = 0
#         batch_datalines_and_process(line_generator, 5, 1, func)
#         self.assertEqual(5, d['batch_count'])
#         self.assertEqual(5, d['total_count'])


# if __name__ == '__main__':
#     unittest.main()
