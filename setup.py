from version import version, required_versions
from setuptools import find_packages, setup


kwargs = dict(
    name='toil-rnaseq',
    version=version,
    description="UC Santa Cruz Computational Genomics Lab's Toil-based RNA-seq workflow",
    author='UCSC Computational Genomics Lab',
    author_email='cgl-toil@googlegroups.com',
    url="https://github.com/BD2KGenomics/toil-lib",
    install_requires=[x + y for x, y in required_versions.iteritems()],
    tests_require=['pytest==2.8.3'],
    package_dir={'': 'src'},
    packages=find_packages('src'),
    entry_points={
        'console_scripts': ['toil-rnaseq = toil_rnaseq.toil_rnaseq:main',
                            'toil-rnaseq-inputs = toil_rnaseq.input_generation:main']})


setup(**kwargs)


print("\n\n"
      "Thank you for installing the UC Santa Cruz Computuational Genomics Lab RNA-seq workflow\n"
      "If you want to run this Toil-based workflow on a cluster in a cloud, please install Toil\n "
      "with the appropriate extras. For example, To install AWS/EC2 support for example, run"
      "\n\n"
      "pip install toil[aws,mesos]%s"
      "\n\n"
      "For more information, please refer to Toil's documentation:\n"
      "https://toil.readthedocs.io")
