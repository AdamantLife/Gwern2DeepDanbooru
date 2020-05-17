import unittest
from Gwern2DeepDanbooru import Directories

import pathlib

class DirectoriesTest(unittest.TestCase):
    """ TestCase for the Directories Helper Class """

    def test_init(self):
        """ Tests basic initialization of the Directories Object. """
        d = Directories()
        self.assertFalse(d._items)

        d = Directories(mydir = None)
        self.assertTrue("mydir" in d._items)

        d = Directories(mydir = dict(value = "myactualdir"))
        self.assertEqual(d.mydir, pathlib.Path("myactualdir"))

        d = Directories(mydir = dict(default = "mydefaultpath"))
        self.assertEqual(d.mydir, pathlib.Path("mydefaultpath"))

        d = Directories(mydir = dict(on_set = lambda path: "alwaysthesamedir"))
        d.mydir = "notthispath"
        self.assertEqual(d.mydir,pathlib.Path("alwaysthesamedir"))

        d = Directories(mydir = dict(not_exists = lambda path: pathlib.Path().resolve()))
        d.mydir = "nonexistingpath"
        self.assertEqual(d.mydir, pathlib.Path().resolve())

    def test_access(self):
        """ Tests various methods of accessing the Directories Object. """
        d = Directories(mydir = None)

        self.assertIsNone(d.mydir)
        self.assertIsNone(d["mydir"])

        self.assertRaises(AttributeError,lambda: d.foobar)
        self.assertRaises(AttributeError,lambda: d['foobar'])

if __name__ == "__main__":
    unittest.main()