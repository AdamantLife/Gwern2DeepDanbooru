import unittest
from Gwern2DeepDanbooru import G2DD

import pathlib

THISDIR = pathlib.Path(__file__).parent
## Sample File Structures for testing
TESTDIR = (THISDIR / "testfilestructure" / "structure1").resolve()
TESTDIR2 = (THISDIR / "testfilestructure" / "structure2").resolve()
## Invalid File Strcutures
BADTESTDIR = (THISDIR / "testfilestructure" / "bad1").resolve()

## Most tests use TESTDIR, so generating TESTDIR paths ahead of time
G_IMG_DIR = (TESTDIR / "gwern_images").resolve()
G_META_DIR = (TESTDIR / "gwern_metadata").resolve()


class G2DD_Case(unittest.TestCase):
    """ TestCase for G2DD """
    @classmethod
    def setUpClass(cls):
        cls.root = TESTDIR
        cls.root2 = TESTDIR2
        cls.badroot = BADTESTDIR
        assert cls.root.exists(), "testfilestructure/structure1 is reqiured to test G2DD"
        assert cls.root2.exists(), "testfilestructure/structure2 is reqiured to test G2DD"
        assert cls.badroot.exists(), "testfilestructure/bad1 is reqiured to test G2DD"

    def test_locate_gwern_image_dir(self):
        g2dd = G2DD(root_dir = self.badroot)
        self.assertIsNone(g2dd.locate_gwern_image_dir())

        g2dd = G2DD(root_dir = self.root)
        ## Make sure gwern_image_dir is not already set
        self.assertIsNone(g2dd.directories.gwern_image_dir)

        ## Test method return
        self.assertEqual(g2dd.locate_gwern_image_dir(), G_IMG_DIR)

        ## Check that g2dd.locate_gwern_image_dir set directories.gwern_image_dir
        self.assertEqual(g2dd.directories.gwern_image_dir, G_IMG_DIR)

        ## TODO: There's no immediately apparent way to check that g2dd.locate_gwern_image_dir 
        ##        immediately returns g2dd.directories.gwern_image_dir when it's available


    def test_locate_gwern_meta_dir(self):
        g2dd = G2DD(root_dir = self.badroot)
        self.assertIsNone(g2dd.locate_gwern_meta_dir())

        g2dd = G2DD(root_dir = self.root)
        ## Make sure gwern_meta_dir is not already set
        self.assertIsNone(g2dd.directories.gwern_meta_dir)

        ## Test method return
        self.assertEqual(g2dd.locate_gwern_meta_dir(), G_META_DIR)

        ## Check that g2dd.locate_gwern_meta_dir set directories.gwern_meta_dir
        self.assertEqual(g2dd.directories.gwern_meta_dir, G_META_DIR)

        ## TODO: There's no immediately apparent way to check that g2dd.locate_gwern_meta_dir 
        ##        immediately returns g2dd.directories.gwern_meta_dir when it's available


        ## Checking for allmetadata.json
        g2dd = G2DD(root_dir = self.root2)
        self.assertEqual(g2dd.locate_gwern_meta_dir(), TESTDIR2/"gwern_metadata")





if __name__ == "__main__":
    unittest.main()