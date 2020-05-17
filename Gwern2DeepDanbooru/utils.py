""" Gwern2DeepDanbooru.methods

    General utilities for Gwern2DeepDanbooru, not necessarily specific to either the Gwern Dataset nor DeepDanbooru

"""

import hashlib
import pathlib
import PIL.Image

class Directories():
    """ Helper class for managing directories

        The managed directories can be accessed either with dot notation (directories.mydir) or via item syntax (directories["mydir"]).
    
        For each directory managed by a Directories object three options can be set:
            default: A callback or value to return when a directory is retrieved and its value is None
            on_set: A callback to call when the value of a directory is about to be set.
                    Should return a value which will be the final value the directory is set to.
            not_exists: A callback to return when a directory is retrieved and its value is a Path that does not exist.
                        It should accept the current not-existing Path instace and should return the value that will be
                        passed on to the caller.

        It is not required to set any of the options: they simply add more control.
        Values set for directories (other than None) will automatically be converted to a pathlib.Path instance made absolute with Path.resolve().

        :param directores: key-value pairs where the key is a name to register with directories (the name used to retrieve the directory)
                            and the value is a dict containing optional keys:
                            * *value* - The initial value for the directory
                            * *default* - As described above
                            * *on_set* - As described above
                            * *not_exists* - As described above

        :return: A new Directories instance
    """
    def __init__(self, **directories):
        """ Creates a new Directories instance """
        self._items = dict()
        for dire,options in directories.items():
            if options is None: options = {}
            self.add_directory(dire, **options)

    def add_directory(self, dire, value = None, default = None, on_set = None, not_exists = None):
        if dire in self._items:
            raise AttributeError('Duplicate directory: "{dire}" is already defined')
        self._items[dire] = dict()
        self.set_default(dire, default)
        self.set_on_set(dire, on_set)
        self.set_not_exists(dire, not_exists)
        self[dire] = value

    def set_default(self,dire, value):
        """ Sets the default to be returned when the [dire] directory is None """
        if dire not in self._items:
            raise AttributeError(f'No directory defined named "{dire}"')
        self._items[dire]["default"] = value

    def set_on_set(self,dire,value):
        """ Sets the callback that will be called when the [dire] directory is set. """
        if dire not in self._items:
            raise AttributeError(f'No directory defined named "{dire}"')
        if value and not callable(value):
            raise ValueError(f"'{value}' does not appear to be callable")
        self._items[dire]["on_set"] = value

    def set_not_exists(self,dire,value):
        """ Sets the callback that will be called when the [dire] directory's value is a non-existing Path """
        if dire not in self._items:
            raise AttributeError(f'No directory defined named "{dire}"')
        if value and not callable(value):
            raise ValueError(f"'{value}' does not appear to be callable")
        self._items[dire]["not_exists"] = value

    def __getitem__(self,name):
        return self.__getattr__(name)

    def __setitem__(self,name,value):
        if name not in self._items:
            raise AttributeError(f'No directory defined named "{name}"')
        on_set = self._items[name].get("on_set")
        if on_set:
            value = on_set(value)
        if value:
            if not isinstance(value, (str, pathlib.Path)):
                raise TypeError(f"Invalid type for directory: {value.__class__}")
            if isinstance(value, str):
                value = pathlib.Path(value).resolve()
        self._items[name]['value'] = value

    def __getattr__(self,name):
        try:
            self.__getattribute__(name)
        except AttributeError:
            if name not in self._items:
                raise AttributeError(f'No directory defined named "{name}"')
            defi = self._items[name]
            value = defi.get("value")
            if value is None and (callback := defi.get("default")):
                value = callback() if callable(callback) else callback
                if isinstance(value, str): value = pathlib.Path(value)
            if value and not value.exists() and (callback := defi.get("not_exists")):
                value = callback(value) if callable(callback) else callback
                self[name] = value
            return value

    def __setattr__(self,name,value):
        if name == "_items" or name not in self._items:
            super().__setattr__(name,value)
            return
        self[name] = value

def get_image_path(metadata, root, mode = "gwern"):
    """ Returns the expected file location for the given metadata based on what format is requested.

        :param metadata: Relevant metadata for the mode; gwern should contain an id key, deepdanbooru should contain md5 key and file_ext keys.
        :type metadata: dict
        
        :param root: The root directory build on: either a gwern-formatted directory or Project/images, according to mode.
        :type root: Union[str, pathlib.Path]

        :param mode: Either "gwern" or "deepdanbooru", defaults to "gwern"
        :type mode: str, optional

        :raises ValueError: If mode is not either "gwern" or "deepdanbooru"

        :return: Expected path to the image
        :rtype: pathlib.Path
    """
    mode = mode.lower()
    if isinstance(root, str): root = pathlib.Path(root)

    if mode == "gwern":
        fname = pathlib.Path(metadata['id']).with_suffix(".jpg")
        subdir = f"{fname.stem[-3:]:0>4}"
    elif mode == "deepdanbooru":
        ext = metadata['file_ext']
        if not ext.startswith("."):
            ext = "."+ext
        fname = pathlib.Path(metadata['md5']).with_suffix(ext)
        subdir = fname.stem[:2]
    else:
        raise ValueError("Invalid Mode")

    return (root/subdir/fname).resolve()

def filtermeta(meta):
    """ Returns the metadata provided with only the keys required for a DeepDanbooru Project remaining. """
    return {key: meta.get(key) for key in MINIMALKEYS if key in meta}

def iter_images(image_dir):
    """ Recursively iterates through the given directory, yielding paths to image files.
    
        :param image_dir: The directory to recursively iterate over
        :type image_dir: pathlib.Path
    """
    for file in image_dir.iterdir():
        if file.is_dir():
            yield from iter_images(file)
        elif file.is_file() and file.suffix.lower() in IMAGE_EXTS:
            yield file
    return

def calculate_md5(image):
    """ Calculates the md5 of the image. This is useful because (as noted by Gwern) sometimes Danbooru's md5 hash is incorrect. 
    
        :param image: the path to the Image to be hashed.
        :type image: Union[str, pathlib.Path]
    """

    imgbytes = PIL.Image.open(image).tobytes()
    return hashlib.md5(imgbytes).hexdigest()
