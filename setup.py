import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="metadata_to_db",
    version="0.0.3",
    author="Matt-Conrad",
    author_email="mattgrayconrad@gmail.com",
    description="Library for storing image metadata from a directory of images to a PostgreSQL DB.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/Matt-Conrad/DicomToDatabase",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3.6",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)