import pandas as pd

if __name__ == '__main__':

    keep = set()
    list = pd.date_range(end=pd.datetime.today(), periods=3, freq='M').tolist()
    for r in list:
        keep.add(str(r.year)+"+"+str(r.month))


    print(keep)
