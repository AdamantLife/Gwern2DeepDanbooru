""" create_testset.py


    This is the utility that was used to create the test dataset supplied with Gwern2DeepDanbooru.
    The utility takes a large dataset and pares it down to a small size (it does this in-place, so make sure to maintain a backup of the initial dataset).
    It performs the following functions:
        Finds images matching the following descriptions:
            A blank image
            A pair of duplicate images
            An image that does not have corresponding metadata
        It earmarks these 4 images and ensures that they will not be removed (so they can be used for testing)
            If one of these categories is missing, a warning will be outputted.
        It reduces the number of image folders down to 10 psuedo-randomly (ensuring that the earmarked images are not removed)
        It randomly reduces each of those folders down to 100 images in each (not counting the special cases)
        It randomly reduces the metadata files down to containing a total of 1503 entries (the 1000 remaining images, 500 entries that do not have images, and the 3 special cases that have metadata)
"""

from Gwern2DeepDanbooru import G2DD, utils, gwern, project
from PIL import Image
import json
import random
import shutil

def main():
    g2dd = G2DD()
    g2dd.initialize_directories()
    
    ## Create an "allmeta" that is simply a set of ids
    allmeta = set()
    for file in gwern.iter_meta(g2dd.directories.gwern_meta):
        with open(file, 'r', encoding = 'utf-8') as f:
            for line in f.readlines():
                obj = json.loads(line)
                allmeta.add(obj['id'])

    keepmeta = set()

    ## Get special images
    blank = None
    dup = None
    nometa = None
    hashes = dict()

    for image in g2dd.gwern.iter_images():
        ## Image without metadata
        if image.stem not in allmeta:
            if not nometa:
                nometa = image
            else:
                ## If we already have a nometa, remove extra nometas
                image.unlink()
            continue

        imgobj = Image.open(image)
        ## Blank Image
        if utils.is_blank_image(imgobj):
            if not blank:
                blank = image
            else:
                ## Remove extra blank images
                image.unlink()
            continue

        ## Duplicate images
        md5 = utils.calculate_md5(imgobj)
        if md5 not in hashes:
            hashes[md5] = image
            continue
        ## Duplicate image
        if not dup:
            dup = (image, hashes[md5])
            continue

        ## Remove extra duplicate images
        image.unlink()


    specialimages = [blank, nometa, *dup]
    ## Don't try to remove nometa, as that will fail
    for image in [blank, *dup]:
        allmeta.remove(image.stem)
        keepmeta.add(image.stem)

    ## Pare down folders
    ## Always keep folders containing specialimages
    keepfolders = list(set([image.parent for image in specialimages]))

    otherfolders = [dire for dire in g2dd.directories.gwern_image.iterdir() if dire.is_dir() and dire not in keepfolders]
    ## (up to) Ten total directories needed (sample can't be larger than population)
    cap = min( len(otherfolders), 10-len(keepfolders) )
    xfolders = random.sample(otherfolders, cap)
    for folder in xfolders: otherfolders.remove(folder)
    keepfolders.extend(xfolders)

    ## Sanity check
    assert len(keepfolders) == 10

    ## Remove extra folders
    for folder in otherfolders:
        shutil.rmtree(folder)

    keepimages = []
    for folder in keepfolders:
        ## Keep 100 images, offset by special images
        specials = [img for img in specialimages if img.parent == folder]
        keepimages.extend(specials)

        ## Make sure special images aren't in the pool
        images = list(utils.iter_images(folder))
        for image in specials: images.remove(image)

        ## Get additional images (up to 100, for reason as noted with folders)
        cap = min(len(images), 100)
        ximages = random.sample(images, cap)

        for image in ximages:
            images.remove(image)
            ## transfer metadata
            allmeta.remove(image.stem)
            keepmeta.add(image.stem)
        keepimages.extend(ximages)

        ## Remove images not selected
        for image in images: image.unlink()

    ## Get up to 500 metadatas that do not have a corresponding image
    cap = min(len(allmeta), 500)
    xmetas = random.sample(list(allmeta), cap)
    for meta in xmetas:
        allmeta.remove(meta)
        keepmeta.add(meta)

    ## Purge original metafiles of metas not selected
    for file in gwern.iter_meta(g2dd.directories.gwern_meta):
        with open(file, 'r+', encoding = 'utf-8') as f:
            ## Not converting to json just yet to make it simpler to write
            metas = f.readlines()
            f.seek(0)
            for line in metas:
                meta = json.loads(line)
                ## If meta still in allmeta (has not been moved to keepmeta) don't write it
                if meta['id'] in allmeta: continue
                f.write(line)
            f.truncate()

    print(f"""Test Set Complete
    Images Kept: {len(keepimages)} images
    Amount of Metadata Preserved: {len(keepmeta)} entries
    Blank Image Included: {bool(blank)}
    Duplicate Images Included: {bool(dup)}
    Image Missing Metadata Included: {bool(nometa)}
""")

if __name__ == "__main__":
    main()