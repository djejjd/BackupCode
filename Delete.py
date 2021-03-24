import pandas as pd

# path = '/home/hadoop/data/test/PersonalInformation.csv'
path01 = '/home/hadoop/data/test/test.csv'


# 删除DiseaseCode中值为66666的数据
def delete_DiseaseCode(df):
    # df1 = df[df['DiseaseCode'].isin(['66666'])]
    df1 = df[~df['DiseaseCode'].isin(['66666'])]
    show_DiseaseCode(df1)
    df1.to_csv(path01, index=False)


# 删除DaysInHos中值小于0的数据
def delete_DaysInHos(df):
    df1 = df.drop(df[df['DaysInHos'].astype(int) < 0].index)
    show_DaysInHos(df1)
    df1.to_csv(path01, index=False)


def delete_lines(df):
    # df.drop('OutHosDate', axis=1, inplace=True)
    # df.drop('InHosDate', axis=1, inplace=True)
    df.to_csv(path01, index=False)


# 检查是否修正
def show_DaysInHos(df):
    t = 0
    for i in df['DaysInHos'].astype(int):
        if i < 0:
            t += 1
            print(df.index(t))
        else:
            continue


def show_DiseaseCode(df):
    for i in df['DiseaseCode']:
        if i == '66666':
            print("error!!!")
        else:
            continue


if __name__ == '__main__':
    data = pd.read_csv(path01, dtype='object', error_bad_lines=False, engine='python', index_col=0)
    # delete_DiseaseCode(data)
    # delete_DaysInHos(data)
    # delete_lines(data)

