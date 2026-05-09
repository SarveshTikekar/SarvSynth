# Capitalize the State name
import sys

def capitalizer(state_name):
    return state_name.title()

if __name__ == "__main__":
    print(capitalizer(sys.argv[1]))