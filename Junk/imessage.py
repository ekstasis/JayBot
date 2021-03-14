import os

script_name = "send_imessage.scpt"
buddies = ["818-231-6661", "678-862-6215", "973-876-8025"]
message1 = "osascript " + script_name + " "
message2 = " 'Automated iMessage Test 1'"

for buddy in buddies:
    message = message1 + buddy + message2
    print(message)
    os.system(message)
    # os.system("osascript send_imessage.scpt 818-231-6661 'sale or buy of > 20,000' ")
