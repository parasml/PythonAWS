#-------------------------------------------------------------------
# Main function
#----------------
from datetime import date, datetime, timedelta
import ECS_handler

bHourTimer = False
bDayTimer = False
bMonthTimer = False
#print("Rollup Time *************************** ", datetime.utcnow())


while True:

    now = datetime.utcnow()

    '''
    # Docker Testing---------------
    nMin = now.minute
    if nMin%5 == 0 and bHourTimer==False:
        print("Hour -------")
        bHourTimer = True
        ECS_handler.ECS_handler(now, 'hour')
    elif nMin%7 == 0:
        bHourTimer = False
    '''
    
    # To fire Hourly Rollup---------------
    if now.minute == 25 and bHourTimer==False:
        #print("Hour -------")
        bHourTimer = True
        ECS_handler.ECS_handler(now, 'hour')
    elif now.minute == 30:   # To rest the boolean -------
        bHourTimer = False


    # To fire Daily Rollup-----------------
    if now.hour == 3 and bDayTimer==False:
        #print("Day -------")
        bDayTimer = True
        ECS_handler.ECS_handler(now, 'day')
    elif now.hour == 5:    # To rest the boolean -------
        bDayTimer = False
    
        
     # To fire Monthly Rollup-----------------  (Will fire at 2nd week)
    dayOfWeek = now.weekday()  #0 is Monday, 5 is saturday------
    if now.day > 1 and  now.day <= 8 and dayOfWeek == 5 and bMonthTimer==False:
        #print("Month -------")
        bMonthTimer = True
        ECS_handler.ECS_handler(now, 'month')
    elif dayOfWeek != 5:    # To rest the boolean -------
        bMonthTimer = False
    


'''
# Local testing -------------------
def Test(event='null', content='null'):
    
    now = datetime.utcnow()
    ECS_handler.ECS_handler(now, 'hour')
'''    
    