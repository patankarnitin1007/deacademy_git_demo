import re

pattern = re.compile(r'(\w+)@(\w+\.\w+)')
match = pattern.search("nitin@gmail.com")
if match:
    print("Found:", match.group())
    print("Username:", match.group(1))
    print("Domain:", match.group(2))