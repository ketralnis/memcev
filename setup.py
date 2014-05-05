from distutils.core import setup, Extension

module1 = Extension('_memcev', sources = ['src/_memcevmodule.c'],
                    libraries=['ev'],
                    include_dirs=['/usr/include', '/usr/local/include', '/opt/local/include'],
                    library_dirs=['/usr/lib', '/usr/local/lib', '/opt/local/lib'])

setup (name = 'Memcev',
        version = '1.0',
        description = 'a memcached client that uses libev',
        ext_modules = [module1],
        package_dir={'': 'src'},
        author="David King",
        license="3-clause BSD <http://www.opensource.org/licenses/bsd-license.php>",
        url="https://github.com/ketralnis/memcev",
        packages = [
            'memcev',
            'memcev.tests',
        ])
