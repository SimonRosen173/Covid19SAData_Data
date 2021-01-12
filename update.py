import scraper
import sa_data
import sys

if __name__ == '__main__':
    print(sys.argv)
    scraper.scrape_data()
    sa_data.preprocess_all()
    if len(sys.argv) > 1 and sys.argv[1] == "pythonanywhere":
        pass
    else:
        sa_data.copy_data_local()