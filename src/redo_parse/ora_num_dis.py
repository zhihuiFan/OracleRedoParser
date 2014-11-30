import sys


def positive(l):
    num = 0
    if l[0] >= 193:
        for n, arg1 in enumerate(l[1:]):
            num += (arg1 - 1) * (100 ** (l[0] - 193 - n))
    elif l[0] <= 62:
        for n, arg2 in enumerate(l[1:-1]):
            num += (arg2 - 101) * (100 ** (62 - l[0] - n))
    return num

if __name__ == '__main__':
    print(positive([int(x, 16) for x in sys.argv[1].split()]))
