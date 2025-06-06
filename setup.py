from setuptools import setup, find_packages
import os

def load_requirements(filename='requirements.txt'):
    """Load requirements from a file."""
    with open(filename, 'r', encoding='utf-8') as f:
        requirements = []
        for line in f:
            # Remove comments and whitespace
            line = line.strip()
            if line and not line.startswith('#'):
                requirements.append(line)
    return requirements

# Read the contents of your README file for a long description
this_directory = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(this_directory, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='kaya',  # Update to your project's name
    version='0.1.0',
    description='Package for pulling and analyzing data from the Kaya climbing website.',
    long_description=long_description,
    long_description_content_type='text/markdown',
    author='Peter Williams',
    author_email='pwilliams272@gmail.com',
    url='https://github.com/pwilliams272/kaya',
    packages=find_packages(),
    include_package_data=True,
    install_requires=load_requirements(), 
    python_requires='>=3.11',
)