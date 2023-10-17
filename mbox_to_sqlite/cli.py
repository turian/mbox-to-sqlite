import click
import sqlite_utils
import mailbox
import hashlib

# TODO: REMOVEME
import random
random.seed()

from tqdm.auto import tqdm
from pprint import pprint

@click.group()
@click.version_option()
def cli():
    "Load email from .mbox files into SQLite"


@cli.command()
@click.argument(
    "db_path",
    type=click.Path(file_okay=True, dir_okay=False, allow_dash=False),
)
@click.argument(
    "mbox_path",
    type=click.Path(file_okay=True, dir_okay=False, allow_dash=False, exists=True),
)
@click.option("--table", default="messages")
def mbox(db_path, mbox_path, table):
    "Import messages from an mbox file"

    db = sqlite_utils.Database(db_path)
    mbox = mailbox.mbox(mbox_path)

    def hsh(s):
        return hashlib.sha224(s.encode("utf-8")).hexdigest()[:16]

    def to_insert():
        for message in tqdm(mbox.itervalues()):
            row = dict(message.items())
            row["payload"] = message.get_payload()

            if "Message-ID" not in row:
                for k in list(row.keys()):
                    if k.lower() == "Message-ID".lower():
                        if "Message-ID" in row:
                            # Don't want multiple of these floating around
                            assert row["Message-ID"] == row[k]
                        row["Message-ID"] = row[k]
                        print(f"Using {(row[k], k)} for Message-ID")
            if "Message-ID" not in row:
                # TODO: Use some hash?
                row["Message-ID"] = "%f" % (random.random() * 1e10)
                print(f"Using RANDOM {row['Message-ID']} for Message-ID")
            assert "Message-ID" in row

            # sqlite3 columns are case insensitive
            # but there might be duplicates with
            # different casing
            # so we md5sum them
            newrow = {hsh(k): v for k, v in row.items()}

            namehashes = [{"name": k, "hash": hsh(k)} for k in row.keys()]
            db["namehashes"].upsert_all(namehashes, pk="name")
            #db["namehashes"].upsert_all(namehashes, alter=True)

            pprint(newrow)
            db[table].upsert(newrow, alter=True, pk=hsh("Message-ID"))

            yield newrow

    for i in to_insert():
        pass
    db[table].upsert_all(to_insert(), alter=True, pk=hsh("Message-ID"))

    #TODO: Do it for all things with this lowercasing
    if not db[table].detect_fts():
        db[table].enable_fts([hsh("payload"), hsh("Subject")], create_triggers=True)
