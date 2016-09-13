from version import version, required_versions
from setuptools import find_packages, setup


kwargs = dict(
    name='toil-rnaseq',
    version=version,
    description="UC Santa Cruz Computational Genomics Lab's Toil-based RNA-seq pipeline",
    author='UCSC Computational Genomics Lab',
    author_email='cgl-toil@googlegroups.com',
    url="https://github.com/BD2KGenomics/toil-lib",
    install_requires=[x + y for x, y in required_versions.iteritems()],
    tests_require=['pytest==2.8.3'],
    package_dir={'': 'src'},
    packages=find_packages('src'),
    entry_points={
        'console_scripts': ['toil-rnaseq = toil_rnaseq.rnaseq_cgl_pipeline:main']})


setup(**kwargs)


print("\n\n"
      "Thank you for installing the UC Santa Cruz Computuational Genomics Lab RNA-seq pipeline! "
      "If you want to run this Toil-based pipeline on a cluster in a cloud, please install Toil "
      "with the appropriate extras. For example, To install AWS/EC2 support for example, run "
      "\n\n"
      "pip install toil[aws,mesos]%s"
      "\n\n"
      "on every EC2 instance. Refer to Toil's documentation at http://toil.readthedocs.io/en/latest/installation.html "
      "for more information."
      % required_versions['toil'])
