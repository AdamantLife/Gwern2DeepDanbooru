""" Gwern2DeepDanbooru.project

    Various utilities related to DeepDanbooru Projects.

"""
from Gwern2DeepDanbooru.constants import *
from Gwern2DeepDanbooru.utils import Directories
import pathlib

class Project():
    def __init__(self,directories = None):
        if directories is None: directories = Directories(root = dict(value = root_dir, default = pathlib.Path.cwd),
                                                          project = dict(value = project_dir, default= lambda: (self.directories.root / "Project"), not_exists = lambda path: path.mkdir() ),
                                                          project_image = dict(default = lambda: (self.directories.project / "images"), not_exists = lambda path: path.mkdir() ))
        if not isinstance(directories, Directories): raise TypeError("Invalid Directiories")
        self.directories = directories

    @property
    def database_path(self):
        return (self.directories.project / PROJECTDATABASENAME).resolve()

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
        if not db: db = self.load_projectdb()
        md5 = image.stem
        return db.execute("""SELECT * from posts WHERE md5=:md5;""", dict(md5=md5)).fetchone()

    def get_metadata_by_md5(self, md5, db = None):
        """ Get image metadata bsed on the image's md5 hash.

            If db is not provided, self.load_projectdb will be used.

            :param md5: the md5 to query for
            :type md5: str
        """
        if not db: db = self.load_projectdb()
        return db.execute("""SELECT * FROM posts WHERE md5=:md5;""", dict(md5 = md5)).fetchone()

    def get_metadata_by_id(self, _id, db = None):
        """ Get image metadata based on the image's id.

            Useful for iteraopability with gwern images.
            If db is not provided, self.load_projectdb will be used.

            :param _id: The id to query for.
            :type _id: int

            :param db: The database to query in, defaults to the return of self.load_projectdb.
            :type db: sqlite3.Connection, optional
        """
        if not db: db = self.load_projectdb()
        return db.execute("""SELECT * FROM posts WHERE id=:id;""", dict(id = _id)).fetchone()

    def update_metadata(self, md5, database = None, **metadata):
        """ Calls update_metadata using self.load_projectdb for the database to update any amount of metadata for a single image.
        
            :param database: A database to use instead of self.load_projectdb (useful to avoid reloading the db repetively)
            :type database: sqlite3.Connection, optional
        """
        if not database: database = self.load_projectdb()
        update_metadata(database, md5, **metadata)

    def load_projectdb():
        """ Utility function to connect to the Project sqlite database """
        return sqlite3.connect(self.database_path)

    def drop_posts(self):
        """ Drops the posts table from a DeepDanbooru Project database. """
        drop_posts(self.load_database)

    def update_is_deleted(self):
        """ Updates the Project's is_deleted column for """

    def initialize_project_database(self):
        """ Creates the Project database and ensures that the posts table is empty. """
        db = self.load_projectdb()
        project.drop_posts(db)
        project.create_posts(db)
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
        project.add_metadata_to_database(db, *allmeta)
        db.commit()
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
        for image in search_func():
            md5, ext = image.stem, image.suffix
            self.update_metadata(md5, database = db, file_ext = ext)

        db.commit()
        db.close()

def drop_posts(database):
    """ Drops the posts table from a DeepDanbooru Project database. """
    database.execute(""" DROP TABLE IF EXISTS posts """)

def create_posts(database):
    """ Creates the posts table for a DeepDanbooru Project database.
    
        There are two differences of note between this method and the Project database
        expected by DeepDanbooru.

        First, md5 is defined as an unique field- this is because images used in the
        database will be saved using their md5 hash and should therefore be unique by
        nature. By formalizing the column's uniqueness, the database avoids inserting
        duplicate images and is more reliably updated if metadata changes.
        
        Secondly, the table created by this module includes rating, score, and is_deleted
        columns: at the moment, DeepDanbooru only makes provisions for those columns in its
        code and does not actually create those columns itself. This module includes them as
        a form of future-proofing.

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

def add_metadata_to_database(database, *metadata, update = True):
    """ Adds the provided metadata to the database. If the md5 for the metadata exists, updates the metadata instead.
    
        :param metadata: metadata to update or into the database
        :type metadata: dict
        
        :param database: database to insert the metadata into
        :type database: sqlite3.Connection

        :param update: Whether to upsert or insert only, default True (update and insert)
        :type update: bool

        :raises AttributeError: if any metadata dict does not contain an md5 key.
    """
    if any(not meta.get("md5") for meta in metadata):
        raise AttributeError("md5 key is required for all metadata entered into Project database")
    cmd = """INSERT INTO posts (id, md5, file_ext, tag_string, tag_count_general, rating, score, is_deleted)
    VALUES (:id, :md5, :file_ext, :tag_string, :tag_count_general, :rating, :score, :is_deleted)"""
    if update:
        cmd += """ON CONFLICT (md5) DO UPDATE SET id=IFNULL(excluded.id, id), file_ext=IFNULL(excluded.file_ext, file_ext),
            tag_string=IFNULL(excluded.tag_string, tag_string), tag_count_general=IFNULL(excluded.tag_count_general, tag_count_general),
            rating=IFNULL(excluded.rating, rating), score=IFNULL(excluded.score, score), is_delete=IFNULL(excluded.is_deleted, is_deleted);"""
    else:
        cmd+=";"

    ## https://stackoverflow.com/a/51237552 suggests that large numbers will crash executemany
    ## (and we expect to be handling large numbers)
    for meta in metadata:
        data = {key: meta.get("key") for key in PROJECTTABLECOLUMNS}
        tags = [tag.get("name") for tag in meta.get("tags",[])]
        data['tag_string'] = " ".join(tags)
        data["tag_count_general"] = len(tags)
        db.execute(cmd, data)
    db.commit();

def update_metadata(database, md5, **metadata):
    """ An alternative to add_metadata_to_database which allows for a selective amount of metadata to be updated.


        :param database: The database which contains the metadata.
        :type database: sqlite3.Connection

        :param md5: The md5 of the image to update
        :type md5: str

        :param **metadata: Valid metadata values to update.
    """
    metadata = {key.lower(): value for key,value in metadata.items()}
    badkeys = ", ".join([key for key in metadata if key not in PROJECTTABLECOLUMNS])
    if badkeys:
        raise ValueError(f"Invalid metadata: {badkeys}")

    updatestr = ", ".join(f"{key}=:{key}" for key in metadata)

    database.execute(f"""UPDATE possts SET {updatestr} WHERE md5=:md5;""", metadata)