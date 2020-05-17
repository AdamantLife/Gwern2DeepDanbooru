import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="Gwern2DeepDanbooru-AdamantLife",
    version="1.0.0",
    author="AdamantLife",
    author_email="contact.adamantmedia@gmail.com",
    description="Automation Utility to reorganize Datasets from Gwern to conform to DeepDanbooru requirements",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/AdamantLife/Gwern2DeepDanbooru",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.8',

    install_requires = [

        ]
)