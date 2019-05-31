import os
'''
by 光城
'''

# 要分割的文件
source_file = '/home/light/mysql/gps1.txt'
save_dir = '/home/light/mysql/gps/gps'
# 定义每个子文件的行数
file_count = 5000000  # 根据需要自定义


def genSubFile(lines, srcName, sub):
    [des_filename, extname] = os.path.splitext(srcName)
    print(des_filename)
    filename = save_dir + '_' + str(sub) + extname
    print('正在生成子文件: %s' % filename)
    with open(filename, 'w') as fout:
        fout.writelines(lines)
        return sub + 1


def split_By_LineCount(filename, count):
    with open(filename, 'r') as fin:
        buf = []
        sub = 1
        for line in fin:
            buf.append(line)

            if len(buf) >= count:
                sub = genSubFile(buf, filename, sub)
                buf = []

        if len(buf):
            genSubFile(buf, filename, sub)
    print("ok")


if __name__ == '__main__':
    split_By_LineCount(source_file, file_count)  # 要分割的文件名和每个子文件的行数
