import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="pyfra",
    version="0.0.1",
    author="Leo Gao",
    author_email="lg@eleuther.ai",
    description="A framework for research code",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/EleutherAI/pyfra",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
    install_requires=[
        'best_download'
    ]
)
