import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="pyfra",
    version="0.2.0",
    author="Leo Gao",
    author_email="lg@eleuther.ai",
    description="A framework for research code",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/EleutherAI/pyfra",
    packages=setuptools.find_packages(),
    include_package_data=True,
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
    install_requires=[
        'best_download',
        'flask',
        'flask-login',
        'flask-wtf',
        'flask-sqlalchemy',
        'flask-migrate',
        'flask-admin',
        'flask-bootstrap',
        'pyjwt',
        'sqlalchemy',
        'wtforms[email]',
        'ansi2html',
        'sqlitedict',
        'colorama',
        'parse',
        'natsort',
        'yaspin',
    ]
)
