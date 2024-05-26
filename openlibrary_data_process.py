"""This script processes the bulk download data from the Open Library project."""

import csv
import ctypes as ct
import os
from tqdm import tqdm
import json


# the folder where the filesforprocessing files are downloaded and extracted to.
INPUT_PATH = "/Users/baber/Downloads/ol_dumps"
OUTPUT_PATH = "./data/processed/"

filesforprocessing = [
    "ol_dump_authors.txt",
    "ol_dump_editions.txt",
    "ol_dump_works.txt",
]


if __name__ == "__main__":
    # See https://stackoverflow.com/a/54517228 for more info on this
    csv.field_size_limit(int(ct.c_ulong(-1).value // 2))

    for file in filesforprocessing:
        with open(
            os.path.join(OUTPUT_PATH, f"{file}.jsonl"),
            "w",
            newline="",
            encoding="utf-8",
        ) as csv_out:
            # csvwriter = csv.writer(
            #     csv_out, delimiter="\t", quotechar="|", quoting=csv.QUOTE_MINIMAL
            # )
            with open(os.path.join(INPUT_PATH, file), "r", encoding="utf-8") as csv_in:
                count = 0
                csvreader = csv.reader(csv_in, delimiter="\t")
                for row in tqdm(
                    csvreader, unit_scale=True, unit="rows", desc=f"Processing {file}"
                ):
                    # if count == 1000:
                    #     break
                    if len(row) > 4:
                        count += 1
                        row: dict = json.loads(row[4])
                        if file == "ol_dump_editions.txt":
                            row.pop("covers", None)
                            row.pop("physical_format", None)
                            row.pop("type", None)
                            row.pop("created", None)
                            row.pop("last_modified", None)
                            row.pop("weight", None)
                            row.pop("physical_dimensions", None)
                            row.pop("latest_revision", None)
                            row.pop("revision", None)
                            work = [x.get("key") for x in row.get("works", [{}])]
                            work = [x for x in work if x]
                            row["works"] = work
                        elif file == "ol_dump_authors.txt":
                            row.pop("created", None)
                            row.pop("type", None)
                            row.pop("last_modified", None)
                            row.pop("revision", None)
                            row.pop("latest_revision", None)
                        elif file == "ol_dump_works.txt":
                            row.pop("created", None)
                            row.pop("covers", None)
                            row.pop("last_modified", None)
                            row.pop("latest_revision", None)
                            row.pop("revision", None)
                            row.pop("type", None)
                            try:
                                author = [
                                    x.get("author", {}).get("key")
                                    for x in row.get("authors", [{}])
                                ]
                                author = [x for x in author if x]
                                row["authors"] = author
                            except:
                                author = [
                                    x.get("author") for x in row.get("authors", [{}])
                                ]
                                author = [x for x in author if x]
                                row["authors"] = author

                        json.dump(row, csv_out, sort_keys=True)
                        csv_out.write("\n")
