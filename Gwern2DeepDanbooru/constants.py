import re
GWERNDIR_RE = re.compile("^0\d{3}$")
GWERNMETADIR_RE = re.compile("^(?P<year>\d{4})(?P<bucket>\d+).json$")
GWERNMETAALL = "allmetadata.json"
## Minimal keys required for a DeepDanbooru Project
MINIMALKEYS = ["id", "tags", "md5", "file_ext", "rating", "score", "is_deleted"]
IMAGE_EXTS = [".jpg", ".jpeg", ".gif", ".png"]

PROJECTDBNAME = "project.sqlite3"
PROJECTTABLECOLUMNS = "id", "md5", "file_ext", "tag_string", "tag_count_general", "rating", "score", "is_deleted"