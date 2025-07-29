from datetime import date, datetime, timedelta
from calendar import monthrange
import os

class DateManager:

    def get_dates(self):
        """
        Returns start_date and end_date based on the current date or environment variables:
        - If SPECIFIC_DATE is True in .env, uses START and END values from .env
        - Otherwise:
          - If today is the 1st day of the month: start_date is the 1st day of the previous month
          - If today is any other day: start_date is the 1st day of the current month
          - end_date is always the current date
        
        Returns:
            tuple: (start_date, end_date) as date objects in format date(year, month, day)
        """
        # Check if specific dates are set in environment variables
        specific_date = os.getenv('SPECIFIC_DATE', 'False').lower() == 'true'
        
        if specific_date:
            # Parse dates from environment variables in format DD/MMYYYY
            start_str = os.getenv('START')
            end_str = os.getenv('END')
            
            if start_str and end_str:
                try:
                    # Parse date in format DD/MMYYYY
                    start_day = int(start_str[:2])
                    start_month = int(start_str[3:5])
                    start_year = int(start_str[5:])
                    
                    end_day = int(end_str[:2])
                    end_month = int(end_str[3:5])
                    end_year = int(end_str[5:])
                    
                    # Create date objects in format date(year, month, day)
                    start_date = date(start_year, start_month, start_day)
                    end_date = date(end_year, end_month, end_day)
                    
                    return start_date, end_date
                except (ValueError, IndexError) as e:
                    print(f"Error parsing dates from environment variables: {e}")
                    # Fall back to default date calculation if parsing fails
        
        # Default date calculation if SPECIFIC_DATE is False or parsing failed
        # Get current date
        today = date.today()
        current_day = today.day
        current_month = today.month
        current_year = today.year
        
        # Set end_date to current date
        end_date = date(current_year, current_month, current_day)
        
        # Determine start_date based on the day of the month
        if current_day == 1:
            # If today is the 1st day of the month, start from 1st day of previous month
            if current_month == 1:
                # If January, go back to December of previous year
                start_date = date(current_year - 1, 12, 1)
            else:
                # Otherwise, go back to previous month of same year
                start_date = date(current_year, current_month - 1, 1)
        else:
            # If today is not the 1st day, start from 1st day of current month
            start_date = date(current_year, current_month, 1)
        
        return start_date, end_date