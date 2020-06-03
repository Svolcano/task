''' yes this is module doc
'''

class T():
    '''
    class doc
    '''

    @classmethod
    def cat(cls):
        print(cls.__module__)
        print(cls.__doc__)



T.cat()
print(__doc__)
