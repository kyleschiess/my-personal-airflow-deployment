import datetime

def previous_quarter(ref):
    if ref.month < 4:
        return datetime.date(ref.year - 1, 12, 31)
    elif ref.month < 7:
        return datetime.date(ref.year, 3, 31)
    elif ref.month < 10:
        return datetime.date(ref.year, 6, 30)
    return datetime.date(ref.year, 9, 30)

def convert_to_snake_case(s):
    new_s = ''
    # check if all the characters are upper case
    if s.isupper():
        new_s = s.lower()
    else:
        for char in s:
            if char == s[0]:
                char = char.lower()
            elif char.isnumeric():
                pass
            elif char.isupper():
                char = '_' + char.lower()

            new_s += char
    
    return new_s
