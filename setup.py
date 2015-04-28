from setuptools import setup

__version__ = "0.1.2.0"

setup(name='pbs_bullet',
        version=__version__,
        description='A script to watch PBS jobs, kill them if they use too much memory, and send pushbullet notifications about them.',
        entry_points={'console_scripts': ['pbs-bullet=pbsbullet.pbs_bullet:main']},
        url='https://github.com/greenape/pbs_bullet',
        author='Jonathan Gray',
        author_email='j.gray@soton.ac.uk',
        license='MIT',
        packages=['pbsbullet'],
        include_package_data=True,
        zip_safe=False
)

with open("pbsbullet/_version.py", "w") as fp:
        fp.write("__version__ = '%s'\n" % (__version__,))