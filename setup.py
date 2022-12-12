from setuptools import setup, find_packages

setup(
    name='j1nuclei',
    version='1.0.3',
    maintainer='JupiterOne',
    packages=find_packages(),
    package_data={"j1nuclei": ['targets_discovery.json']},
    install_requires=['jupiterone', 'requests'],
    url='https://github.com/jupiterOne/j1nuclei',
    license='MIT License',
    author='JupiterOne',
    author_email='sacha.faut@jupiterone.com',
    description='J1Nuclei is a CLI tool demonstrating how JupiterOne platform can automate and learn from other tools. It automates everyday security tasks of scanning endpoints for vulnerabilities.',
    entry_points={
        'console_scripts': [
            'j1nuclei = j1nuclei.cli:main'
        ],
    },
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Intended Audience :: Information Technology',
        'Intended Audience :: System Administrators',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Python',
        'Topic :: Security',
    ],
)
