from distutils.core import setup, Extension

module1 = Extension('_memcev', sources = ['src/_memcevmodule.c'],
                    libraries=['ev'],
                    include_dirs=['/opt/local/include'],
                    library_dirs=['/opt/local/lib'])

setup (name = 'Memcev',
        version = '1.0',
        description = 'a memcached client that uses libev',
        ext_modules = [module1])
