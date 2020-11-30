""" Gwern2DeepDanbooru.multiprocessing

    A module for parallel execution of the G2DD.create_project_immediate method to speed up processing at the cost of system resources.

    Work Flow:
                          Main Process
                               |
        -----------------------------------------------
        |              |              |               |
    fileworker -> metaworker -> uploadworker   duplicateworker

    Progression:
    * The Main Process establishes the metadata files to be processed and places them in the metafiles Queue
    * The fileworkers processes the metafiles Queue and pushes json objects to the metadata Queue
    * The uploadworker takes processed metadata from the metadata Queue and uploads it into the database
    * The duplicateworker watches the database for duplicates: whenever a duplicate appears, it handles the duplicate

    Communication:
    * After initializing all child processes, the Main Process intermittently poll the fileworkers until they have all exited
    * Once all the fileworkers have exited, the Main Process sends a signal to the metaworkers indicating that no more metadata will be added to the metadata queue
        * After receiving the signal, the metaworkers will exit once the metadata queue is empty
    * Once all metaworkers have exited, the uploadworker is signaled indicating that no more metadata will be added to the upload queue
        * After receiving the signal, the uploadworker will exit once the upload queue is empty
    * Once the uploadworker has exited, the duplicateworker is signaled indicating that no more metadata will be uploaded to the database
        * After receiving the signal, the duplicate worker will exit once it confirms that there are no duplicates remaining
    * Once the duplicateworker has exited, the Main Process will exit
"""

from Gwern2DeepDanbooru import G2DD, project, gwern
from queue import Empty
import multiprocessing as mp
from pprint import pprint
import sqlite3

def emptyqueue(queue, maxitems = 25):
    """ Depletes a queue until it is empty or maxitems is reached. If maxitems <= 0, depletes until queue is empty. """
    items = []
    while True:
        try:
            items.append(queue.get(1))
            if 0 < maxitems <= len(items):
                return items
        except Empty: return items

def init_db(project_path):
    """ Uniform method for initializing the database in autocommit and Write Ahead Logging mode """
    g2dd = G2DD(project_path)
    g2dd.project.initialize_directories()
    db = sqlite3.connect(g2dd.project.database_path, isolation_level = None)
    db.execute(""" PRAGMA journal_mode=wal; """)
    return g2dd, db

def workerdistribution(processes):
    """ Given a number of desired child processes, divide those children among the workers based on their weights """

    ## Required workers must have exactly n-number of processes
    required = {"duplicate_worker": 1,
                "upload_worker": 1}

    ## Remaining processes are distributed to the other workers based on their weight (min 1 process)
    ## Any remainder is reapplied to the highest weight workers.
    weighted = {"fileworker":1,
               "metaworkers":2}
    total_weights = sum(weighted.values())

    total = sum(required.values()) + len(weighted)
    if processes < total:
        raise ValueError(f"At least {total} child processes are required for G2DD.multiprocessing")
    remaining_processes = processes
    output = {}
    for worker, count in required.items():
        output[worker] = count
        remaining_processes -= count

    weighted_processes = remaining_processes

    for worker,weight in sorted(weighted, key = lambda tup: tup[1]):
        ## take the floor of number of processes remaining for weighted processes * the proportion of this worker's weight
        ## The result must be at least 1
        ## The result cannot be more than the remaining processes
        procs = min(max(int(weighted_processes * (weight / total_weights)), 1), remaining_processes)
        output[worker] = procs
        remaining_processes -= procs

    ## Any remaining processes (due to rounding) are distributed evenly from highest weight to lowest
    while remaining_processes:
        for worker in sorted(weighted, reverse = True):
            if remaining_processes:
                output[worker]+=1
                remaining_processes -= 1
            else: ## No more remaining_processes
                break

    return output
     
def upload_worker(project_path, upload, pipe):
    """ Child Process responsible for uploading to the database (as sqlite is single-threaded) """
    g2dd, db = init_db(project_path)

    running = True
    items = []
    try:
        while running or items:
            ## If pipe has data, finish running
            running = not pipe.poll()

            ## There will be a maximum of 25 items with no additional argument for emptyqueue
            items = emptyqueue(upload)
            if items:
                project.add_metadata_to_database(db, *items, update_tags = False)

    finally:
        db.commit()
        db.close()

def duplicate_worker(project_path, pipe):
    """ Child Process responsible for finding duplicates and combining them """
    g2dd, db = init_db(project_path)

    running = True
    duplicates = []
    while running and duplicates:
        ## If pipe has data, finish running
        running = not pipe.poll()
        duplicates = project.find_duplicate_images(db)
        if duplicates:
            for md5 in duplicates:
                rows = g2dd.project.get_metadata_by_md5(md5, rowid = True)
                initial_row = rows[0]
                alltags = list(set(
                        sum(
                            (row['tag_string'].split() for row in rows)
                        , [])
                                   ))
                db.execute("""UPDATE posts SET tag_string = :tag_string WHERE rowid = :rowid;
                DELETE FROM posts WHERE md5 = :md5 AND rowid != :rowid;""", dict(tag-string = " ".join(alltags), rowid = initial_row['rowid'], md5 = md5))

def create_project_immediate(path, processes = 4):
    g2dd = G2DD(path)
    g2dd.initialize_directories()

    workers = workerdistribution(processes)
    print(f"Worker Distribution among {processes} processes:")
    pprint(workers)
    
    ## File Workers takes metadata files from the metafiles Queue, opens each one, minimizes each item
    ## and places the item on the metadata Queue
    fileworkers = []

    ## Meta Workers take metadata from the metadata Queue, locates the image related to the
    ## metadata, updates the md5 hash if necessary, and handles any exception related to the image.
    ## When it is done (assuming it has not discarded the metadata for any reason), it places the metadata 
    ## on the projectdata Queue
    metaworkers = []

    ## Upload Worker takes processed metadata and uploads it to the database
    uploadworker = []

    ## Duplicate Worker monitors the database for duplicates and combines them
    duplicateworker = []
    

    metafiles = mp.Queue()
    metadata = mp.Queue()
    projectdata = mp.Queue()
    uploaddata = mp.Queue()
    for file in gwern.iter_meta(g2dd.directories.gwern_meta): metfiles.put(file)

    



    

class MultiG2DD(G2DD):
	def __init__(self,*args,**kw):
		super().__init__(*args, **kw)

	def create_project_immediate(selfignore_blanks = True, combine_duplicates = True, commit_batch = 1000):
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


if __name__ == "__main__":
    import pathlib
    path = pathlib.Path()
    create_project_immediate(path)