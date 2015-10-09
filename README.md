# Bon_F15
This directory contains my 3 main projects including
1) SeedFinderAPI - this folder contain scripts to download data from API and convert it into a nice csv table.
2) pdftotxt - this folder contains a script that used to convert a pdf file into a csv file full of texts categorized by articles
	- usage: python pdf2txt_gen.py input_pdf_file output_csv_file
3) cosine_sim - this folder contains a script that is used to calculate the cosine similarity between two strains in each group. It takes in a big table of product descriptions of strains as an input and output in a nice csv format. This code also uses multiprocessors to parallelize the work.
	- usage: python cosine_strain_parallel.py input_file output_file #chunks
