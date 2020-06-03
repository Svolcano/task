from abc import ABC
import inspect


class Meta(type):

    def __new__(cls, name, bases, attrs):
        print("i am Meta", name, bases, attrs)
        res = type.__new__(cls, name, bases, attrs)
        print(inspect.getmodule(res).__name__, '======')
        res.label = 'hello world'
        print(cls.__module__)
        with open("aaa.txt", 'a') as w:
            w.write("hello\n")

        return res


class Meta2(type):

    def __new__(cls, name, bases, attrs):
        print("i am Meta2", name, bases, attrs)
        res = type.__new__(cls, name, bases, attrs)

        res.label = 'ni hao'
        return res

class B1(metaclass=Meta):
    pass


class B12(type(ABC), type(B1)):
    pass


class B2(ABC):

    def __new__(cls, *args, **kwargs):
        print('i amd B2', args, kwargs)
        hell = "asdfasdf"
        return hell

# print('--'*40)
# b = B1()
# print(b.label)
# print('--'*40)
# b2 = B2()
# print(b2)


class C(B1, B2, metaclass=B12):
    pass
# print('--'*40)
# c = C()
# print(c)
