""" Gwern2DeepDanbooru

    This module provides utilies for working with and manipulate data taken (or atleast formatted in the same matter as) Gwern
        with the intent of compiling it into a Project as expected by DeepDanbooru.


    Most utilities in this module can be easily accessed by creating a G2DD instance in a directory with two subdirectories: one
        containing image files bucketed as per the Gwern layout, and the other containing either a single metadata file named
        "allmetadata.json" or several json files formatted, truncated, and labeled per Gwern's format.

    The simplest way to convert a Gwern formatted dataset directly to a DeepDanbooru Project is to use the G2DD instance's
        func:create_project_immediate: this will create the Project and move any images from the Gwern image directory
        without any intermediate steps.

    A Project can also be created from an allmetadata.json file using func:G2DD.create_project function: various methods
        are available for creating the allmetadata.json file, depending on how much metadata you want included in it.
"""

import json
import pathlib
import PIL.Image
import shutil
import sqlite3
from typing import Union
from Gwern2DeepDanbooru.constants import *
from Gwern2DeepDanbooru import utils, gwern, project

class G2DD():
    """ G2DD is a highlevel Object representing the current runtime environment
    
        :param root_dir: The directory to use to find and create directories that are not otherwise defined, defaults to pathlib.Path
        :type root_dir: Union[str,pathlib.Path], optional

        :param gwern_image_dir: The parent directory to the Gwern Image subdirectorys. If not defined, G2DD can attempt to locate the directory itself.
        :type gwern_image_dir:  Union[str,pathlib.Path], optional

        :param gwern_meta_dir: The directory containing the Gwern Metadata Jsons. If not defined, G2DD can attempt to locate the directory itself.
        :type gwern_meta_dir:  Union[str,pathlib.Path], optional

        :param project_dir: The project directory for the current runtime, defaults to (self.root_dir / "Project"). If the directory does not exist, G2DD will create it when it is referenced.
        :type project_dir:  Union[str,pathlib.Path], optional

        :return: A new G2DD Instance
    """
    def __init__(self, root_dir: Union[str,pathlib.Path] = None,
                 gwern_image_dir: Union[str,pathlib.Path] = None, gwern_meta_dir: Union[str,pathlib.Path] = None,
                 project_dir: Union[str,pathlib.Path] = None):
        """ Initialize a new G2DD runtime environment. """
        self.directories = utils.Directories(root = dict(value = root_dir, default = pathlib.Path.cwd))
        self.gwern = gwern.Dataset(self.directories, gwern_image_dir = gwern_image_dir, gwern_meta_dir = gwern_meta_dir)
        self.project = project.Project(self.directories, project_dir = project_dir)

    def initialize_directories(self):
        """ Convenience function to locate both gwern_image and gwern_data, and create project and project_image if they do not already exist. """
        self.gwern.locate_gwern_image_dir()
        self.gwern.locate_gwern_meta_dir()
        self.directories.project
        self.directories.project_image

    def locate_image(self, metadata):
        """ Returns the image file if it is located in self.directories.gwern_image or self.directories.project_image.

            Will only check directories that are defined and exists.
            To check the gwern_image directory, metadata should contain an "id" value.
            To check the project_image directory, metadata should contain both "md5" and "file_ext" 
            
            :param metadata: Metadata for the image trying to be retrieved.
            :type metadata: dict

            :return: The filepath to the image if it exists, otherwise None.
            :rtype: Union[pathlib.Path,None]
        """
        try:
            if (location:= self.gwern.locate_image(metadata)):
                return location
        except FileNotFoundError:
            try:
                if (location := self.project.locate_image(metadata)):
                    return location
            except FileNotFoundError: pass

    def clean_images(self):
        """ Removes any image from self.directories.gwern_image and self.directories.project_image that do not have
            metadata in allmetadata or the Project Database.

            If either allmetadata or Project Database do not exist, it will simply skip checking them. If both
            are missing, a FileNotFound error will be raised.

            If either gwern_image or project_image do not exist, it will simply be skipped. If both are missing,
            then this method simply returns, as there are no images to check.

            :raises FileNotFoundError: If both allmetadata and Project Database are missing.

        """
        find_methods = []
        db = None
        try: db = self.project.load_projectdb()
        except FileNotFoundError: pass
        else: find_methods.append(lambda image: self.project.get_image_metadata(image, db= db))

        try: allmetadata = self.load_allmetadata()
        except FileNotFoundError: pass
        else: find_methods.append(lambda image: allmetadata.get(image.stem))

        if not find_methods:
            raise FileNotFoundError("clean_images requires at least either allmetadata or a Project Database")

        dirs = []
        if (gwern_image := self.directories.gwern_image) and gwern_image.exists():
            dirs.append(gwern_image)
        if (project_image := self.directories.project_image) and project_image.exists():
            dirs.append(project_image)

        if not dirs:
            return

        for dire in dirs:
            for image in utils.iter_images(dire):
                imgloc = None
                for method in find_methods:
                    if (imgloc := method(image)): break
                if not imgloc:
                    print("Deleting", image.name)
                    image.unlink()

        if db: db.close()

    def update_image_exts(self):
        """ Updates the file_ext in allmetadata and the Project's Database.

            Gwern normalizes images into 512x512px JPGs for machine learning: if you are using these prepared images,
            then the metadata may need to be updated to reflect this.
        """
        allmetadata, db = {}, None
        try: allmetadata = self.load_allmetadata()
        except FileNotFoundError: pass
        try: db = self.project.load_projectdb()
        except FileNotFoundError: pass

        try: gwern_images = self.gwern.iter_images()
        except FileNotFoundError: pass
        else:
            for image in gwern_images:
                _id,ext = image.stem, image.suffix
                md5 = None
                if allmetadata and _id in allmetadata:
                    md5 = (meta := allmetadata[_id]).get("md5")
                    meta['file_ext'] = ext
                if not md5:
                    md5 = self.project.get_image_by_id(_id, db)
                    if not md5: continue
                    md5 = md5['md5']
                self.project.update_metadata(md5, file_ext = ext, database = db)

        try: project_images = self.project.iter_images()
        except FileNotFoundError: pass
        else:
            for image in project_images:
                md5,ext = image.stem, image.suffix
                _id = None
                if db:
                    metadata = self.project.get_metadata_by_md5(md5, db = db)
                    if not metadata: continue
                    metadata = metadata[0]
                    self.project.update_metadata(md5, file_ext = ext, database = db)
                    _id = metadata.get("id")
                if _id and _id in allmetadata:
                    allmetadata[_id]['file_ext'] =ext

        if db:
            db.commit()
            db.close()

        if allmetadata:
            self.save_allmetadata(allmetadata)

    def minimize_metafiles(self):
        """ Calls self.gwern.minimize_metafiles """
        self.gwern.minimize_metafiles()

    def create_allmetadata(self, output = None):
        """ Calls self.gwern.create_allmetadata """
        self.gwern.create_allmetadata(output = output)

    def create_allmetadata_minimal(self, output = None, search_func = None):
        """ Calls self.gwern.create_allmetadata_minimal. Uses G2DD.locate_image by default instead. """
        if search_func is None: search_func = self.locate_image
        self.gwern.create_allmetadata_minimal(output = output, search_func = search_func)

    def load_allmetadata(self, returntype = "dict"):
        """ Calls self.gwern.load_allmetadata """
        return self.gwern.load_allmetadata(returntype = returntype)

    def save_allmetadata(self, metadata):
        """ Calls self.gwern.save_allmetadata """
        self.gwern.save_allmetadata(metadata)

    def clean_allmetadata(self, search_func = None):
        """" Calls self.gwern.clean_allmetadata. Uses G2DD.locate_image by default instead. """
        if search_func is None: search_func = self.locate_image
        self.gwern.clean_allmetadata(search_func = search_func)



    def initialize_project_database(self):
        """ Calls self.project.initialize_project_database. """
        self.project.initialize_project_database()

    def create_project(self, move = True):
        """ Creates a DeepDanbooru project from allmetadata.json.

            Creates the Project's database from allmetadata and moves any images in self.directories.gwern_image
                that are included in allmetadata to self.directories.project_image. Images can be copied instead
                by setting the move parmater to False.
            Images are renamed to reflect their md5 when they are moved/copied into project_image.
            If the Project database already exists, it will be overwritten.

            :param move: Whether to move images from gwern_image to project_image, defaults to True. If False, the images will be copied instead.
            :type move: bool
        """
        self.create_project_database()
        self.move_gwern_images_to_project(move = move)
        self.project.create_tags_file()

    def create_project_database(self):
        """ Converts allmetadata.json into a Project database for DeepDanbooru
        
            This will create an sqlite file named project.sqlite3 which is formatted per DeepDanbooru's specifications
                and populated with the metadata from self.directories.allmetadata. If project/project.sqlite3 already
                exists, it's posts table will be dropped and recreated.
        """
        allmeta = self.load_allmetadata(returntype = "list")
        self.initialize_project_database()
        db = self.project.load_projectdb()
        project.add_metadata_to_database(db, *allmeta)
        db.commit()
        db.close()

    def move_gwern_images_to_project(self, move = True):
        """ Moves and renames images from self.directories.gwern_image to self.directories.project_image.

            Images are renamed from the gwern format (id-based) to the DeepDanbooru format (md5-based).
            If move if False, images are copied instead of moved.
            Images should have their md5 hash updated before hand.
            Only images that are contained in allmetadata will be moved.
            If self.directories.gwern_image does not exist, a FileNotFoundError will be raised.

            :param move: Whether to move or copy the image, defaults to move (True). If False, the image will be copied instead.
            :param type: bool, optional

            :raises FileNotFoundError: If self.directories.gwern_image does not exist.
        """
        if not (gwern_image := self.directories.gwern_image) or not gwern_image.exists():
            raise FileNotFoundError('gwern_image directory not defined or does not exist (to automatically locate it call g2dd.gwern.locate_image_dir() first)')

        project_image = self.directories.project_image

        allmetadata = self.load_allmetadata()
        for image in self.gwern.iter_images():
            _id = image.stem
            if _id in allmetadata:
                target = utils.get_image_path(allmetadata[_id], project_image, mode="deepdanbooru")
                if move: image.rename(target)
                else: shutil.copy(image, target)

    def create_project_immediate(self, ignore_blanks = True, combine_duplicates = True, commit_batch = 1000):
        """ Creates a DeepDanbooru Project from a Gwern-formatted Dataset without any intermediate steps.

            This method, like create_allmetadata_minimal, is more resource efficient than performing
            the intermediate steps individually, though obviously it does not allow for any extra
            preprocessing not already implemented.

            :param ignore_blanks: Whether or not to include blank (counterproductive) images, defaults to True.
                                    If False, subsequent metadata will be discarded.
            :param type: bool

            :param combine_duplicates: If two images are identical, combine their tags, defaults to True
            :param type: bool

            :param commit_batch: Number of database operations to perform before commiting, defaults to 1000.
                                    If commit_batch is not a positive integer, no commits will be made until the very end.
            :param type: int
        """
        self.initialize_directories()
        self.initialize_project_database()
        db = self.project.load_projectdb()

        project_image = self.directories.project_image

        ## This method now only commits every commit_batch
        commit_counter = 0
        if not isinstance(commit_batch, int):
            raise TypeError("commit_batch must be an integer")

        ## Adapted from create_allmetadata_minimal
        for file in gwern.iter_meta(self.directories.gwern_meta):
            with open(file, 'r', encoding = 'utf-8') as mf:
                for line in mf.readlines():

                    if not (line := line.strip()): continue
                    ## Minimize metadata
                    meta = utils.filtermeta(json.loads(line))
                    ## Clean metadata
                    if not (imagepath := self.gwern.locate_image(meta)): continue
                    ## Load image
                    image = PIL.Image.open(imagepath)
                    ## Check blank
                    if ignore_blanks and utils.is_blank_image(image):
                        continue
                    ## Update md5
                    meta['md5'] = utils.calculate_md5(image)

                    ## Move image
                    target = utils.get_image_path(meta, project_image, mode = "deepdanbooru")
                    ## Duplicate md5
                    update_duplicate = False
                    if target.exists():
                        ## Duplicate Image and updating meta
                        if combine_duplicates and utils.is_same_image(image, target):
                            update_duplicate = True
                        ## Discard if not Duplicate or not updating meta
                        else: continue
                    if not update_duplicate:
                        target.parent.mkdir(exist_ok = True)
                        imagepath.rename(target)

                        ## Save image metadata
                        project.add_metadata_to_database(db, meta, update_tags = False)
                    else:
                        projectmeta = self.project.get_metadata_by_md5(meta['md5'], db = db)[0]
                        tags = projectmeta['tag_string'].split()
                        tags += [tag['name'] for tag in meta['tags']]
                        tag_string = " ".join(sorted(set(tags)))
                        project.update_metadata(db, meta['md5'], tag_string = tag_string, update_tags = False)

                    ## commit_batch <= 0 means do not commit until the end
                    if commit_batch > 0:
                        ## Data has been added to the db
                        commit_counter+= 1
                        ## If we have reached the batch limit for commits
                        if commit_counter >= commit_batch:
                            db.commit()
                            commit_counter = 0

        ## One final commit
        db.commit()
        db.close()
        self.project.create_tags_file()