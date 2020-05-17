import json
import pathlib
from Gwern2DeepDanbooru.constants import *
from Gwern2DeepDanbooru import utils
from Gwern2DeepDanbooru.utils import Directories

class Dataset():
    def __init__(self, directories = None):
        if directories is None: directories = Directories(root = dict(value = root_dir, default = pathlib.Path.cwd),
                                       gwern_image = dict(value = gwern_image_dir),
                                       gwern_meta = dict(value = gwern_meta_dir),
                                       gwern_allmeta = dict(default = lambda: (self.directories.gwern_meta / GWERNMETAALL).resolve() if self.directories.gwern_meta else None),)
        if not isinstance(directories, Directories): raise TypeError("Invalid Directiories")
        self.directories = directories

    def locate_gwern_image_dir(self):
        """ Searches for a directory formatted like the Gwern Danbooru Image Dataset.

            If self.directories.gwern_image is already set, it will be returned instead.
            Recurses through self.directories.root looking for the first directory containing
            multiple subdirectories matching the regex (^0\d{3}$) (Gwern's bucketing pattern).
            It sets that directory as self.directories.gwern_image and returns it.

            If no matching directory can be found, None will be returned.

            Note that this methods does not check that the subdirectories actually contain images.
        """
        if self.directories.gwern_image: return self.directories.gwern_image
        def recurse(dire):
            dirs = [d for d in dire.iterdir() if d.is_dir()]
            matching = [d for d in dirs if GWERNDIR_RE.match(d.name)]
            if len(matching) > 1:
                return dire
            for d in dirs:
                if (dire := recurse(d)):
                    return dire

        if (dire := recurse(self.directories.root)):
            self.directories.gwern_image = dire
        return dire

    def locate_gwern_meta_dir(self):
        """ Searches for a directory formatted like the Gwern Danbooru Image Dataset Metadata.

            If self.directories.gwern_meta is already set, it will be returned instead.
            Recurses through self.directories.root looking for the first directory containing
            either a single file named "allgwernmetadata.json" or multiple jsons files matching
            the regex (^(?P<year>\d{4})(?P<bucket>\d+).json$) (Gwern's json naming pattern). It
            sets that directory as self.directories.gwern_meta and returns it.

            If no matching directory can be found, None will be returned
        """
        if self.directories.gwern_meta: return self.directories.gwern_meta
        gwern_image = self.directories.gwern_image
        def recurse(dire, include_image = False):
            ## Don't recurse through gwern_image on the initial search
            ## to save time (assume images and metadata are in separate dirs)
            if dire == gwern_image and not include_image: return
            dirs = []
            jsons = []
            for f in dire.iterdir():
                if f.is_file() and f.suffix == ".json":
                    if f.name == GWERNMETAALL:
                        return dire
                    if GWERNMETADIR_RE.match(f.name):
                        jsons.append(f)
                elif f.is_dir():
                    dirs.append(f)

            if len(jsons) > 1:
                return dire

            for d in dirs:
                if (dire := recurse(d, include_image = include_image)):
                    return dire

        if (dire := recurse(self.directories.root)):
            self.directories.gwern_meta = dire
        else:
            if (dire := recurse(gwern_image, include_image = True)):
                self.directories.gwern_meta = dire

        return dire

    def locate_image(self, metadata):
        """ Given metadata, attempt to locate the image in self.directories.gwern_images.

            Returns the result of utils.get_image_path(metadata, self.directories.gwern_image)
            if the location exists, otherwise returns None.
        """
        if not (gwern_image := self.directories.gwern_image) or not gwern_image.exists():
            raise FileNotFoundError('gwern_image directory not defined or does not exist (to automatically locate it call g2dd.gwern.locate_image_dir() first)')
        if (path := utils.get_image_path(metadata, gwern_image, mode = "gwern")).exists():
            return path

    def iter_images(self):
        """ Iterates over the images in self.directories.gwern_image, yielding images.
        
            Utility function for utils.iter_images with the gwern_image directory.

            :raises FileNotFoundError: if gwern_image does not exist
        """
        if not (gwern_image := self.directories.gwern_image) or not gwern_image.exists():
            raise FileNotFoundError('gwern_image directory not defined or does not exist (to automatically locate it call g2dd.gwern.locate_image_dir() first)')
        yield from utils.iter_images(self.directories.gwern_image)

    def minimize_metafiles(self):
        """ If your computer has limited resources, call this method before creating allmetadata.json in order to
                remove all non-essential metadata from the gwern metadata files. This will reduce each file size
                by about 1/3 (from about 370mb a file to 270mb) which will have a proportionate effect on the
                size of allmetadata.json when it is created.

            The resulting files will be in the structural format as they were originally (so that they are still
                capatible with other tools) but will only the following keys:
                * id
                * tags
                * md5
                * file_ext
                * rating
                * score
                * is_deleted

            This method will overwrite the files where they are located: as always, make sure you have a backup if you may need the data in the future.

            :raises FileNotFoundError: If self.directories.gwern_meta is not defined
        """
        if not (gwern_meta := self.directories.gwern_meta) or not gwern_meta.exists():
            raise FileNotFoundError("minimize_gwern_metafiles requires the gwern_meta directory")

        for file in iter_meta(self.directories.gwern_meta):
            data = load_meta(file)
            data = list(map(utils.filtermeta, data))
            datastring = "\n".join(json.dumps(meta) for meta in data)
            with open(file, 'w', encoding = 'utf-8') as f:
                f.write(datastring)

    def create_allmetadata(self, output = None):
        """ Combines all Gwern Metadata Jsons into a single json called "allmetadata.json".

            This function will automatically overwrite an existing file called "allmetadata.json" in the output directory.
        
            :param output: The target output directory, defaults to self.directories.gwern_meta
            :type output: Union[str, pathlib.Path], optional

            :raises FileNotFoundError: If self.directories.gwern_meta is None or does not exist.
            :raises FileNotFoundError: If the output directory does not exist.
        """
        if not self.directories.gwern_meta or not self.directories.gwern_meta.exists():
            raise FileNotFoundError('gwern_meta directory not defined (to automatically locate it call g2dd.gwern.locate_meta_dir() first)')

        if not output:
            output = self.directories.gwern_meta
        if isinstance(output,str):
            output = pathlib.Path(str).resolve()
        if not output.exists():
            raise FileNotFoundError("Output directory does not exist")
        output = (output / GWERNMETAALL).resolve()

        ## Reorganized so to lower amount of memory and cpu utilized
        with open(output, 'w', encoding = 'utf-8') as f:
            f.write("[")
            first = True
            for file in iter_meta(self.directories.gwern_meta):
                ## If not first, we need to add a comma and newline at the end of the previous file's output
                if not first:
                    f.write(",\n")
                with open(file, 'r', encoding = 'utf-8') as mf:
                    f.write(",\n".join([line.strip() for line in mf.readlines()]))

            f.write("]")

    def create_allmetadata_minimal(self, output = None, search_func = None):
        """ Combines the functionality of Dataset.minimize_metafiles, create_allmetadata, and clean_allmetadata in order to save as many system resources as possible.

            See the individual methods for more information on what this entails.

            :param output: The target output directory, defaults to self.directories.gwern_meta
            :type output: Union[str, pathlib.Path], optional

            :raises FileNotFoundError: If self.directories.gwern_meta is None or does not exist.
            :raises FileNotFoundError: If the output directory does not exist.
        """
        if not self.directories.gwern_meta or not self.directories.gwern_meta.exists():
            raise FileNotFoundError('gwern_meta directory not defined or does not exist (to automatically locate it call g2dd.gwern.locate_meta_dir() first)')

        if not output:
            output = self.directories.gwern_meta
        if isinstance(output,str):
            output = pathlib.Path(str).resolve()
        if not output.exists():
            raise FileNotFoundError("Output directory does not exist")

        output = (output / GWERNMETAALL).resolve()

        if search_func is None: search_func = self.locate_image

        with open(output, 'w', encoding = 'utf-8') as f:
            f.write("[")
            first = True
            for file in iter_meta(self.directories.gwern_meta):
                with open(file, 'r', encoding = 'utf-8') as mf:
                    for line in mf.readlines():

                        if not (line := line.strip()): continue
                        ## Minimize metadata
                        meta = utils.filtermeta(json.loads(line))
                        ## Clean metadata
                        if not search_func(meta): continue

                        ## If not first, we need to add a comma and newline at the end of the previous lines's data
                        if not first:
                            f.write(",\n")
                        else: first = False
                        f.write(json.dumps(meta))

            f.write("]")


    def load_allmetadata(self, returntype = "dict"):
        """ Helper function for calling load_allmetadata. Unlike load_allmetadata,
                this function returns a dict by default as that format is more useful for this class.

            :param returntype: Either "list" or "dict" (per load_allmetadata), defaults to "dict"
            :type returntype: str

            :return: A list or dict of metadata from gwern_meta/allmetadata.json
            :rtype: Union[list,dict]
        """
        return load_allmetadata(self.directories.gwern_meta, returntype = returntype)

    def save_allmetadata(self, metadata):
            """ Convenience function to save metadata to self.directories.gwern_allmeta """
            with open(self.directories.gwern_allmeta, 'w', encoding = 'utf-8') as f:
                json.dump(metadata, f)

    def clean_allmetadata(self, search_func = None):
        """ Removes metadata from allmetadata that belongs to an image that cannot be located with locate_image.

            Note that this method does not enforce the existance of either self.directories.gwern_image or
                self.directories.project_image: if neither of these are defined or exists, all metadata
                will be removed from allmetadata.

            :param search_func: The search function to confirm that the metadata's image exists, defaults to self.locate_image.
            :type search_func: function, optional

            :raises FileNotFoundError: If self.directories.gwern_meta/allmetadata.json does not exist.
        """
        if search_func is None: search_func = self.locate_image
        metadata = self.load_allmetadata()
        
        for _id,image in list(metadata.values()):
            if not search_func(image):
                del metadata[_id]

        self.save_allmetadata(metadata)

    def clean_images(self):
        """ Removes any image from self.directories.gwern_image if it does not have a metadata entry in allmetadata.

            NOTE: images in gwern_image are expected to be named by their id (which is what is crossreferenced with allmetadata)


        
            :raises FileNotFoundError: If self.directories.gwern_image is not defined or does not exist.
            :raises FileNotFoundError: If allmetadata.json does not exist.
        """
        if not (gwern_image := self.directories.gwern_image) or not gwern_image.exists():
            raise FileNotFoundError('gwern_image directory not defined or does not exist (to automatically locate it call g2dd.gwern.locate_image_dir() first)')

        allmeta = self.load_allmetadata()

        for file in utils.iter_images(gwern_image):
            if file.stem not in allmeta:
                print("Deleting", file.name)
                file.unlink()

    def update_image_exts(self, search_func = None):
        """ Updates allmetadata's file_ext keys for each image found.

            Gwern normalizes images into 512x512px JPGs for machine learning: if you are using these prepared images, then
                the metadata may need to be updated to reflect this.

        :param search_func: The generator used to find the images, defaults to self.iter_images
        :type search_func: function, optional
        """
        if search_func is None: search_func = self.iter_images
        allmetadata = self.load_allmetadata()
        for image in search_func():
            _id, ext = image.stem, image.suffix
            if _id in allmetadata:
                allmetadata[_id]['file_ext'] = ext

        self.save_allmetadata(allmetadata)

def iter_meta(gwern_meta_dir):
    """ Iterates over the provided gwern_meta_dir, yielding all jsons that match the gwern metadata pattern
    
        :param gwern_meta_dir: The directory containg gwern metadata jsons.
        :type gwern_meta_dir: pathlib.Path

        :return: Yields a json file
    """
    for file in gwern_meta_dir.iterdir():
        if file.is_file() and GWERNMETADIR_RE.match(file.name):
            yield file

def load_meta(meta_file):
    """ Loads a gwern-formatted json file (which is assumed to be malformed) and returns a list of the metadata.

        :param meta_file: The gwern-formatted metadata file. It is expected to contain a newline-separated list of json objects.
        :type meta_file: Union[str, pathlib.Path]

        :return: A list of metadata dicts.
        :rtype: list
    """
    output = []
    with open(meta_file, 'r', encoding = 'utf-8') as f:
        for line in f.readlines():
            output.append(json.loads(line.strip()))
    return output

def load_allmetadata(gwern_meta_dir, returntype = "list"):
    """ Loads the metadata contained in gwern_meta_dir/allmetadata.json

        :param gwern_meta_dir: The directory containing allmetadata.json (expected to be alongside the truncated jsons it was built on)
        :type gwern_meta_dir: Union[str, pathlib.Path]

        :param returntype: How to return the data: either "list" or "dict", defaults to "list". If "dict", the keys will be the metadata's "id" key.
        :type returntype: str, optional

        :raises ValueError: If returntype is not either "list" or "dict"

        :return: The metadata contained in gwern_meta_dir/allmetadata.json
        :rtype: Union[list, dict], depending on the value of returntype
    """
    if (returntype := returntype.lower()) not in ["list", "dict"]:
        raise ValueError('returntype must be either "list" or "dict"')
    with open(gwern_meta_dir / GWERNMETAALL, 'r', encoding = 'utf-8') as f:
        result = json.load(f)
        if returntype == "list": return result
        return {meta['id']:meta for meta in result}

#def find_tags(file, allimages):
#    data = load_metadata(file)

#    for img in data:
#        if (_id := img['id']) in allimages:
#            if allimages[_id]:
#                print("""DUPLICATE ID:
#{img}
#{allimages[_id]}
#------------------""")
#                continue
#            allimages[_id] = img

#def gather():
#    allimages= {}
    
#    print("Gathering Images")
#    get_images(IMGPATH)
    
#    print("Gathering Jsons")
#    for file in iter_metapath():
#        find_tags(file, allimages)

#    print("Saving to File")
#    with openfile(METAOUTPUT, 'w') as f:
#        json.dump(allimages, f)

#    print("Done")

#def check_tags():
#    data = load_metadata(METAOUTPUT)
#    missing = []

#    for img,tags in data.items():
#        if not tags:
#            print(f"Missing tags for {img}")
#            missing.append(img)

#    with openfile(MISSING, 'w') as f:
#        json.dump(missing, f)    

#def check_for_metadata_for_imgs(*imgs):
#    def check(file):
#        data = load_metadata(file)
#        for img in imgs:
#            if img in data:
#                imgs.pop(img)
#                print(f"Tags found for {img}: {data[img]}")

#    imgs = list(imgs)
#    if not imgs:
#        imgs = load_metadata(MISSING)
#    if not imgs:
#        print("No Missing Tags")
    
#    for file in iter_metapath():
#        check(file)

#def remove_imgs_missing_metadata():
#    data = load_metadata(MISSING)
#    for img in list(data):
#        path = getimagepath(img)
#        if not path.exists():
#            print(f"COULD NOT FIND IMG ID {img}: {path}")
#            continue
#        print(f"Removing {path}")
#        path.unlink()

#def purge_missing_images(data = None):
#    if data is None:
#        data = load_metadata(METAOUTPUT)

#    for img in list(data):
#        k,d = getimagepath(img), getimagepath(img, "deepbooru")
#        if not k.exists() and not d.exists():
#            print(f"{img} not found: Purging")
#            del data[img]
#    with openfile(METAOUTPUT, 'w') as f:
#        json.dump(data,f)

#    print("Done")

#def reformat_img_structure():
#    """ Restructure gwern download to match DeepDanbooru expected values.

#        danbooru bulk download from gwern is sorted by final 3 digits of id.
#        DeepDanbooru expects first 2 digits of MD5 (but does not confirm valid MD5) subdirectories in a folder called "images".
#        Uses METAOUTPUT to gather images
#    """
#    data = load_metadata(METAOUTPUT)

#    i = 0 
#    for img, metadata in data.items():
#        src = getimagepath(img)
#        dest = getimagepath(img, "deepbooru")
#        if not src.exists():
#            if not dest.exists():
#                print(f"Cannot find image: {img}\n{metadata}")
#        else:
#            dest.parent.mkdir(exist_ok = True)
#            src.rename(dest)
#        if not ((i := i+1) % 10000):
#            print(f"Moving image number",i)

#    print("Done")

#def lookup_img_metadata(img):
#    try:
#        img.exists()
#    except:
#        try:
#            img = int(img)
#        except:
#            raise ValueError("Invalid image")
#    else:
#        img = int(img.stem)

#    data = load_metadata(METAOUTPUT)
#    try:
#        print(data[str(img)])
#    except KeyError:
#        print("No Image Metadata with that ID")
    
    
##gather()
##check_tags()
##check_for_metadata_for_imgs()
##remove_imgs_missing_metadata()
##purge_missing_images()
##reformat_img_structure()
#build_db()
        
##lookup_img_metadata(10060)
