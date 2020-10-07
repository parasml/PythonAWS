# ---------------------------------------------------
# Main class
#--------------
import recordCount
from datetime import datetime, timedelta


def main():
    now = datetime.utcnow()
    recordCount.recordCount(now)

#----------------------------------------------------
# Local Call
#---------------

if __name__ == "__main__":
    main()


