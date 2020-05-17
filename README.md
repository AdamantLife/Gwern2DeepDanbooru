# Gwern2DeepDanbooru
Reorganizes Danbooru Datasets from Gwern to be valid for DeepDanbooru

#### Format Comparison
<table>
    <thead>
        <tr>
            <th></th>
            <th>Gwern</th>
            <th>DeepDanbooru</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td style="font-weight:bold;">File Structure</td>
            <td>Images and Metadata have separate Subdirectories</td>
            <td>Metadata is a single file alongside the <i>Images</i> subdirectory</td>
        </tr>
        <tr>
            <td style="font-weight:bold;">Image Subdirectories</td>
            <td>Images are bucketed into 4-digit, Zero padded subdirectories based on the final 3 digits of the Image's ID.</td>
            <td>Images are bucketed into subdirectories based on the first 2 digits of the Image's md5 hash.<sup>[1](#note1)</sup></td>
        </tr>
        <tr>
            <td style="font-weight:bold;">Images</td>
            <td>Images are available at full size, but a script is provided to downsample the images to 512x512px jpg format for machine learning. Downloads are also available in this format.
<br>Image filenames are their ids.</td>
            <td>Images are assumed to be named with their md5 hash.</td>
        </tr>
        <tr>
            <td style="font-weight:bold;">Metadata</td>
            <td>Metadata is truncated into multiple json files.
<br>The files are not strictly json compliant as they are formatted as newline-separated json objects (the "array" of objects is missing encompassing brackets and comma separation)
<br>All metadata available via Danbooru's API is included.
</td>
            <td>Metadata for training is stored in a single SQLite database in a table called <i>posts</i>.
<br>The table's columns are <b>id</b>, <b>file_ext</b>, <b>md5</b>, <b>tag_string</b>, and <b>tag_count_general</b>.
<br>There is some infrastructure for <b>rating</b> and <b>score</b>, but they are not documented.</td>
        </tr>
    </tbody>
    <tfoot style="font-size:small;font-style:italic;">
        <tr><td colspan="3">
<a name="note1">1</a>: Gwern notes in their introduction to the dataset that Danbooru's MD5 hashes are not always correct: accordingly, using MD5 hashes may cause issues
</td></tr>
    </tfoot>
</table>

## Installation
A *pypi* package has not yet been compiled, so instead either clone this repository or use:
<br>
```pip install git+https://github.com/AdamantLife/Gwern2DeepDanbooru```

## Basic Usage
While Gwern2DeepDanbooru offers a variety of methods, the baseline usage can be achieved very simply via the commandline:
```
cd {gwern data location}
python Gwern2DeepDanbooru run
```

This command will:
* create a new directory called ```Project/``` in the current work directory
* compile all metadata into a single, valid json file
* move all images (which have metadata available) within this directory to the appropriate subdirectory in ```Project/Images/```
* create ```Project/project.sqlite3```
* and populate the database with the required data to train DeepDanbooru.

*(Remember to always maintain a backup of your data in case you wish to use the Gwern data in its original format)*

## Documentation
TODO