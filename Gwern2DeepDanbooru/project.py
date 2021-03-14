""" Gwern2DeepDanbooru.project

    Various utilities related to DeepDanbooru Projects.

"""
from Gwern2DeepDanbooru.constants import *
from Gwern2DeepDanbooru.utils import Directories, DBContext
import csv
import pathlib
import sqlite3

class Project():
    def __init__(self,directories = None, project_dir = None):
        if directories is None: directories = Directories(root = dict(default = pathlib.Path.cwd))
        if not isinstance(directories, Directories): raise TypeError("Invalid Directories")

        if not directories.is_directory("project"):
            directories.add_directory("project", value = project_dir, default= lambda: (self.directories.root / "Project"), not_exists = lambda path: path.mkdir())
        if not directories.is_directory("project_image"):
            directories.add_directory("project_image", default = lambda: (self.directories.project / "images"), not_exists = lambda path: path.mkdir() )
        if not directories.is_directory("project_tags"):
            directories.add_directory("project_tags", default = lambda: (self.directories.project / "tags.txt"))
        self.directories = directories

    @property
    def database_path(self):
        return (self.directories.project / PROJECTDBNAME).resolve()

    @property
    def db_context(self):
        return  DBContext(self.load_projectdb)

    def initialize_directories(self):
        self.directories.project
        self.directories.project_image

    def locate_image(self, metadata):
        """ Given metadata, attempt to locate the image in self.directories.project_image.

            Returns the result of utils.get_image_path(metadata, self.directories.project_image, mode = "deepdanbooru)
            if the location exists, otherwise returns None.
        """
        if (path := utils.get_image_path(metadata, self.directories.project_image, mode = "deepdanbooru")).exists():
            return path

    def get_image_metadata(self, image, db = None):
        """ Given an image path, return the image's metadata from the Project's database.

            If the image does not have metadata, return None.
            If the db is not  provided, self.load_projectdb will be used.

            :param image: The path to an image
            :type image: Union[str, pathlib.Path]

            :param db: The database to search in, defaults to the return of self.load_projectdb.
            :type db: sqlite3.Connection, optional
        """
        if (not_supplied := db is None):
            db = self.load_projectdb()
        try:
            md5 = image.stem
            result = db.execute("""SELECT * from posts WHERE md5=:md5;""", dict(md5=md5)).fetchone()
        finally:
            if not_supplied: db.close()
        return result

    def get_metadata_by_md5(self, md5, rowid = False, db = None):
        """ Get image metadata bsed on the image's md5 hash.

            If db is not provided, self.load_projectdb will be used.

            :param md5: the md5 to query for
            :type md5: str

            :param rowid: Also return the rowid, default False. Useful when dealing with duplicates.
            :type rowid: bool

            :param db: The database to query in, defaults to the return of self.load_projectdb.
            :type db: sqlite3.Connection, optional

            :return: A list of sqlite3.Row objects (or the current row_factory)
            :rtype: list
        """
        rowid_string = ""
        if rowid: rowd_string = ", rowid"
        if (not_supplied := db is None):
            db = self.load_projectdb()
        try:
            results = db.execute(f"""SELECT *{rowid_string} FROM posts WHERE md5=:md5;""", dict(md5 = md5)).fetchall()
        finally:
            if not_supplied:
                db.close()
        return results

    def get_metadata_by_id(self, _id, rowid = False, db = None):
        """ Get image metadata based on the image's id.

            Useful for iteraopability with gwern images.
            If db is not provided, self.load_projectdb will be used.

            :param _id: The id to query for.
            :type _id: int

            :param rowid: Also return the rowid, default False. Useful when dealing with duplicates.
            :type rowid: bool

            :param db: The database to query in, defaults to the return of self.load_projectdb.
            :type db: sqlite3.Connection, optional
                        
            :return: A list of sqlite3.Row objects (or the current row_factory)
            :rtype: list
        """
        rowid_string = ""
        if rowid: rowd_string = ", rowid"
        if not db: db = self.load_projectdb()
        return db.execute(f"""SELECT *{rowid_string} FROM posts WHERE id=:id;""", dict(id = _id)).fetchone()

    def update_metadata(self, md5, database = None, **metadata):
        """ Calls update_metadata using self.load_projectdb for the database to update any amount of metadata for a single image.
        
            :param database: A database to use instead of self.load_projectdb (useful to avoid reloading the db repetively)
            :type database: sqlite3.Connection, optional
        """
        if not database: database = self.load_projectdb()
        update_metadata(database, md5, **metadata)

    def load_projectdb(self):
        """ Utility function to connect to the Project sqlite database """
        if not self.database_path.exists(): raise FileNotFoundError("Project Database has not been created yet")
        conn = sqlite3.connect(self.database_path)
        conn.row_factory = sqlite3.Row
        return conn

    def drop_posts(self):
        """ Drops the posts table from a DeepDanbooru Project database. """
        drop_posts(self.load_projectdb())

    def drop_tags(self):
        """ Drops the tags utility table from a DeepDanbooru Project database. """
        drop_tags(self.load_projectdb())

    def initialize_project_database(self):
        """ Creates the Project database and ensures that the posts table is empty. """
        if not self.database_path.exists():
            self.database_path.touch()
        db = self.load_projectdb()
        drop_posts(db)
        create_posts(db)
        drop_tags(db)
        create_tags(db)
        db.commit()
        db.close()

    def create_project_database(self):
        """ Converts allmetadata.json into a Project database for DeepDanbooru
        
            This will create an sqlite file named project.sqlite3 which is formatted per DeepDanbooru's specifications
                and populated with the metadata from self.directories.allmetadata. If project/project.sqlite3 already
                exists, it's posts table will be dropped and recreated.
        """
        allmeta = self.load_allmetadata(returntype = "list")
        self.initialize_project_database()
        db = self.load_projectdb()
        try:
            project.add_metadata_to_database(db, *allmeta)
            db.commit()
        finally:
            db.close()

    def update_image_exts(self, search_func = None):
        """ Updates the Project database's file_ext values for each image found.

            Gwern normalizes images into 512x512px JPGs for machine learning: if you are using these prepared images, then
                the metadata may need to be updated to reflect this.

        :param search_func: The generator used to find the images, defaults to self.iter_images
        :type search_func: function, optional
        """
        if search_func is None: search_func = self.iter_images
        db = self.load_projectdb()
        try:
            for image in search_func():
                md5, ext = image.stem, image.suffix
                self.update_metadata(md5, database = db, file_ext = ext)

            db.commit()
        finally:
            db.close()

    def create_tags_file(self, limit = None):
        """ Creates the tags.txt file required by DeepDanbooru based on the Project's database. This will silently overwrite the tags file if it exists.

            :param limit: Minimum number of images required to include a tag in the list, defaults to None which includes all tags.
            :type limit: Union[int,None]
        """

        db = self.load_projectdb()
        try:
            tags = get_tags_dict(db)
            if limit:
                tags = [tag for tag,count in tags.items() if count >= limit]
            else:
                tags = list(tags)

            with open(self.directories.project_tags, 'w') as f:
                f.write("\n".join(tags))
        finally:
            db.close()

    def output_tags_count(self, file = None):
        """ Outputs all tags with their count as a csv file (useful for determining the limit for creating the tags file)

            :param file: The filename to output to, defaults to None. If not supplied "tag_count.csv" will be used.
            :type file: Union[str, pathlib.Path]
        """
        db = self.load_projectdb()
        try:tags = [{"tag":k, "count":v} for k,v in get_tags_dict(db).items()]
        finally: db.close()
        if not file: file = "tag_count.csv"
        with open(file, 'w', newline = "") as f:
            writer = csv.DictWriter(f, fieldnames = ["tag", "count"])
            writer.writeheader()
            writer.writerows(tags)

    def iter_metadata(self):
        """ A generator to iterate over the metadata stored in the posts table """
        db = self.load_projectdb()
        try:
            cur = db.execute("""SELECT * FROM posts;""")
            while (row := cur.fetchone()):
                yield row
        finally:
            db.close()

    def sync_tags(self, md5):
        """ Syncs the tag_string stored in the posts table with the tags table """
        db = self.load_projectdb()
        try:
            sync_tags(db, md5)
        finally:
            db.close()

def drop_posts(database):
    """ Drops the posts table from a DeepDanbooru Project database. """
    database.execute(""" DROP TABLE IF EXISTS posts; """)

def drop_tags(database):
    """ Drops the tags utility table from a DeepDanbooru Project database. """
    database.execute(""" DROP TABLE IF EXISTS tags; """)

def create_posts(database):
    """ Creates the posts table for a DeepDanbooru Project database.
    
        Unlike the formal table declaration by DeepDanbooru, the table created by this module
        includes rating, score, and is_deleted columns: at the moment, DeepDanbooru only makes
        provisions for those columns in its code and does not actually create those columns
        itself. This module includes them as a form of future-proofing.

        :param database: The database to create the table in. The posts table should not already exist (call drop_posts() if necessary).
        :type database: sqlite3.Connection
    """
    database.execute("""CREATE TABLE posts (
id INTEGER,
md5 TEXT UNIQUE,
file_ext TEXT,
tag_string TEXT,
tag_count_general INTEGER,
rating TEXT,
score INTEGER,
is_deleted BOOLEAN
);""")

def create_tags(database):
    """ Creates the tags table used by Gwern2DeepDanbooru.

        The tags table is a table that maps the foreign key posts.md5 to a tag (string)
        and is used for some querying situations where parsing tag_string would be inefficient.

        :param database: The database to create the table in. The tags table should not already exist (call drop_tags() if necessary).
        :type database: sqlite3.Connection
    """
    database.execute("""CREATE TABLE tags (
    md5 TEXT REFERENCES posts(md5),
    tag TEXT NOT NULL,
    UNIQUE (md5, tag) ON CONFLICT IGNORE
);""")


def add_metadata_to_database(database, *metadata, update = True, update_tags = True):
    """ Adds the provided metadata to the database. If the md5 for the metadata exists, updates the metadata instead.
    
        :param metadata: metadata to update or into the database
        :type metadata: dict
        
        :param database: database to insert the metadata into
        :type database: sqlite3.Connection

        :param update: Whether to upsert or insert only, default True (update and insert)
        :type update: bool

        :param update_tags: Whether to update the tags table, defaults to True (perform update)
        :type update_tags: bool

        :raises AttributeError: if any metadata dict does not contain an md5 key.
    """
    if any(not meta.get("md5") for meta in metadata):
        raise AttributeError("md5 key is required for all metadata entered into Project database")
    cmd = """INSERT INTO posts (id, md5, file_ext, tag_string, tag_count_general, rating, score, is_deleted)
    VALUES (:id, :md5, :file_ext, :tag_string, :tag_count_general, :rating, :score, :is_deleted)"""
    if update:
        cmd += """ON CONFLICT (md5) DO UPDATE SET id=IFNULL(excluded.id, id), file_ext=IFNULL(excluded.file_ext, file_ext),
            tag_string=IFNULL(excluded.tag_string, tag_string), tag_count_general=IFNULL(excluded.tag_count_general, tag_count_general),
            rating=IFNULL(excluded.rating, rating), score=IFNULL(excluded.score, score), is_deleted=IFNULL(excluded.is_deleted, is_deleted);"""
    else:
        cmd+=";"

    ## https://stackoverflow.com/a/51237552 suggests that large numbers will crash executemany
    ## (and we expect to be handling large numbers)
    for meta in metadata:
        data = {key: meta.get(key) for key in PROJECTTABLECOLUMNS}
        tags = [tag.get("name") for tag in meta.get("tags",[])]
        data['tag_string'] = " ".join(tags)
        data["tag_count_general"] = len(tags)
        database.execute(cmd, data)
        remove_tags(database,data['md5'])
        if update_tags:
            update_tags(database, data['md5'], data['tag_string'].split())

def update_metadata(database, md5, update_tags = True, **metadata):
    """ An alternative to add_metadata_to_database which allows for a selective amount of metadata to be updated.


        :param database: The database which contains the metadata.
        :type database: sqlite3.Connection

        :param md5: The md5 of the image to update
        :type md5: str

        :param update_tags: Whether to update the tags table, defaults to True (perform update)
        :type update_tags: bool

        :param **metadata: Valid metadata values to update.
    """
    metadata = {key.lower(): value for key,value in metadata.items()}
    badkeys = ", ".join([key for key in metadata if key not in PROJECTTABLECOLUMNS])
    if badkeys:
        raise ValueError(f"Invalid metadata: {badkeys}")

    if "tag_string" in metadata:
        ## Force this to be correct
        metadata['tag_count_general'] =len(metadata['tag_string'].split())

    updatestr = ", ".join(f"{key}=:{key}" for key in metadata)
    substitutes = metadata.copy()
    substitutes['md5'] = md5

    database.execute(f"""UPDATE posts SET {updatestr} WHERE md5=:md5;""", substitutes)

    if "tag_string" in metadata and update_tags:
        remove_tags(database,md5)
        update_tags(database, md5, metadata['tag_string'].split())


def get_tags_dict(database):
    """ Returns a dict where the keys are tags and the value is the number of images which contain those tags """
    tags = database.execute("""SELECT tag, COUNT(md5) AS "count"
        FROM tags
        GROUP BY tag;""").fetchall()
    return {tag['tag']: tag['count'] for tag in tags}

def remove_tags(database, md5):
    """ Removes all linked tags in the tags table for the given md5 """
    database.execute("""DELETE FROM tags WHERE md5=?;""", (md5,))

def sync_tags(database, md5):
    remove_tags(database, md5)
    tagstring = database.execute("""SELECT tag_string FROM posts WHERE md5=?""", (md5,)).fetchone()[0]
    tags = tagstring.split()
    update_tags(database, md5, tags)
    
def update_tags(database, md5, tags):
    tag_substitute = {f"tag{i}":tag for i,tag in enumerate(tags)}
    tag_inserts = ",".join(f"(:md5, :{tagnumber})" for tagnumber in tag_substitute)
    tag_substitute["md5"] = md5
    database.execute(f"""INSERT INTO tags (md5, tag) VALUES {tag_inserts};""", tag_substitute)

def find_duplicate_images(database):
    """ A method to locate duplicate image in the posts Table. Returns a list of md5 hashes.


        :param database: The database to inspect for duplicates
        :type database: sqlite3.Connection

        :returns: A list of md5 hashes
        :rtype: list
    """
    results = database.execute("""SELECT md5, COUNT(*) c FROM posts GROUP BY md5 having c > 1;""").fetchall()
    return [row['md5'] for row in results]