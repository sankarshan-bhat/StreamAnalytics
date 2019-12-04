from memory_profiler import profile



@profile
def memory_function():
    print("jatin")
    y=5
    x = y+1
    print(x)


memory_function()