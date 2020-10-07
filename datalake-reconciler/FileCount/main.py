# ---------------------------------------------------
# Main class
#--------------
import fileCount
from datetime import datetime, timedelta


def main():
    now = datetime.utcnow()
    fileCount.fileCount(now)

#----------------------------------------------------
# Local Call
#---------------

if __name__ == "__main__":
    main()
