from distutils.core import setup


CLASSIFIERS = ['Development Status :: 3 - Alpha',
               'Intended Audience :: Education',
               'Intended Audience :: Science/Research',
               'License :: OSI Approved :: MIT License',
               'Operating System :: OS Independent',
               'Programming Language :: Python :: 3',
               'Programming Language :: Python :: 3.3',
               'Programming Language :: Python :: 3.4',
               'Programming Language :: Python :: 3.5',
               'Programming Language :: Python :: 3.6',
               'Topic :: Scientific/Engineering',
               ]

DESCRIPTION = "Python API for the Pecan Street Dataport."

PACKAGES = ['pecanpy']


setup(name='pecanpy',
      packages=PACKAGES,
      version='0.1.0-alpha',
      description=DESCRIPTION,
      author='David R. Pugh',
      author_email='david.pugh@kapsarc.org',
      url='https://github.com/KAPSARC/pecanpy',
      license='LICENSE.rst',
      classifiers=CLASSIFIERS
      )
