from pybuilder.core import use_plugin, init, Project, Author

use_plugin("python.core")
use_plugin("python.unittest")
use_plugin("python.install_dependencies")
use_plugin("python.flake8")
use_plugin("python.distutils")

name = "sparkle-hypothesis"
summary = "Use the power of hypothesis property based testing in PySpark tests"
description = """
Data heavy tests benefit from Hypothesis to generate your data and desinging your tests. Sparkle-hypothesis makes it 
easy to use Hypothesis strategies to generate dataframes.
"""
default_task = ["clean", "analyze", "publish"]
version = "1.0.2.dev"

url = "https://github.com/machielg/sparkle-hypothesis/"
license = "GPLv3+"


authors = [Author("Machiel Keizer Groeneveld", "machielg@gmail.com")]
@init
def set_properties(project: Project):
    project.build_depends_on('pyspark')
    project.depends_on('hypothesis')
    project.depends_on('sparkle-test')
    project.depends_on('sparkle-session')

    project.set_property("distutils_classifiers", [
        "License :: OSI Approved :: GNU General Public License v3 or later (GPLv3+)",
        "Environment :: Console",
        'Programming Language :: Python :: 3.6',
        'Topic :: Software Development :: Testing'
    ])
