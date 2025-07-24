"""
------------------------
Key changes (Optimized version of loadOldSubmissionValues.py)
1. One shared MySQL connection-pool (no connect-per-row).
2. Batch INSERT with INSERT IGNORE + executemany().
3. Re-use a prepared statement for speed.
"""

import csv
import datetime as dt
import os
import mysql.connector

from dotenv import load_dotenv
from mysql.connector import pooling, IntegrityError

from parser import isValidSubmissionCSV
from downloader import retrieveUPC

load_dotenv()
HOST      = os.getenv("MYSQL_HOST")
USER      = os.getenv("MYSQL_USER")
PASSWORD  = os.getenv("MYSQL_PASSWORD")
DB        = os.getenv("MYSQL_DATABASE")

# Build a small connection‑pool we can borrow from
POOL = pooling.MySQLConnectionPool(
    pool_name="excl_pool",
    pool_size=4,           # adjust if we need more concurrency
    host=HOST,
    user=USER,
    password=PASSWORD,
    database=DB,
    autocommit=False       # we’ll commit once per batch
)

# Helper functions
def fetch_rows():
    """Grab all candidate rows once."""
    conn = POOL.get_connection()
    cur  = conn.cursor()

    cur.execute(
        """SELECT submission_id, submission, item_file, gtin
           FROM alec_site.general_form_subs
           WHERE submission_date >= '2025-06-01' 
           AND submission_date < '2025-07-01'"""        
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


def insert_upcs(batch):
    """INSERT IGNORE so duplicates don’t blow up the whole batch."""
    if not batch:
        return
    conn = POOL.get_connection()
    cur  = conn.cursor(prepared=True)

    cur.executemany(
        "INSERT IGNORE INTO alec_site.general_item_file_upc (submission_id, upc) "
        "VALUES (%s, %s)",
        batch
    )
    conn.commit()
    cur.close()
    conn.close()
    batch.clear()
    print("!!! pushed 500 into db!!!")


def checkSQL(submissionID, value): #check for duplicates in the sql db
    conn = POOL.get_connection()
    cursor  = conn.cursor(prepared=True)
    cursor.execute("""SELECT submission_id, upc FROM alec_site.general_item_file_upc 
                   WHERE submission_id = %s AND upc = %s LIMIT 1""", (submissionID, value))
    exists = cursor.fetchone() is not None

    cursor.close()
    conn.close()
    return exists

# main function
def main():
    now        = dt.datetime.now()
    timestamp  = now.strftime("%Y-%m-%d at %H-%M-%S")
    os.makedirs("Unsuccessful Submission Rows", exist_ok=True)
    log_path   = f"Unsuccessful Exclusion Rows/{timestamp}.csv"

    with open(log_path, "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(
            ["Submission Number", "Submission ID", "itemFile", "Error"]
        )

        rows  = fetch_rows()
        batch = []
        BATCH_SIZE = 1000          

        for sub_id, sub_num, item_file, gtin in rows:
            if not (item_file and item_file.strip()) and not gtin:
                writer.writerow([sub_num, sub_id, "—", "Both GTIN and itemFile not present (skipped)"])
            elif not (item_file and item_file.strip()):
                if not checkSQL(sub_id, gtin):
                    print(f"({sub_num}) itemFile is not present, using GTIN instead: {gtin}")
                    writer.writerow([sub_num, sub_id, "—", f"Used GTIN ({gtin}) as UPC"])
                try:
                    batch.append((sub_id, gtin))
                    if len(batch) >= BATCH_SIZE:
                        insert_upcs(batch)
                except mysql.connector.Error as e:
                    print(f"MySql Error: {e}")
                    writer.writerow([sub_num, sub_id, "—", "No item_file, used GTIN"])
                else:
                    print(f"{sub_id} itemFile is not present, but used GTIN ({gtin}) already") 
                    writer.writerow([sub_num, sub_id, "—", "GTIN already present as UPC (handled and skipped)"])
                continue

            elif not isValidSubmissionCSV(item_file):
                print(f"({sub_num}) has a bad URL")
                writer.writerow([sub_num, sub_id, item_file, "Bad URL"])
                continue

            try:
                upcs, bad_upcs = retrieveUPC(item_file)

                if upcs:
                    if not checkSQL(sub_id, gtin):
                        batch.append(sub_id, gtin)
                    for upc in upcs:
                        print(f"Found UPC: {upc}")
                        if not checkSQL(sub_id, upc):
                            batch.append((sub_id, upc))
                            if len(batch) >= BATCH_SIZE:
                                insert_upcs(batch)
                        else:
                            print(mysql.connector.IntegrityError)
                            writer.writerow([sub_num, sub_id, item_file, "Already Exists in Table"])

                    # Log invalid UPCs
                    for bad in bad_upcs:
                        writer.writerow(
                            [sub_num, sub_id, item_file, f"Bad UPC Value - {bad}"]
                        )
                
                if upcs:
                    print(f"Queued {len(upcs)} UPCs for submission {sub_id}")

                if not upcs and not bad_upcs:
                    writer.writerow(
                        [sub_num, sub_id, item_file, "Missing UPC Column"]
                    )

            except IntegrityError:
                writer.writerow([sub_num, sub_id, item_file, "Already Exists"])
            except Exception as err:
                writer.writerow(
                    [sub_num, sub_id, item_file, f"Failed: {err}"]
                )

        # Push whatever is left
        insert_upcs(batch)


if __name__ == "__main__":
    main()
