import sa_data
import sys

if __name__ == '__main__':
    sa_data.preprocess_all()
    if sys.argv and sys.argv[0] == "pythonanywhere":
        pass
    else:
        sa_data.copy_data_local()