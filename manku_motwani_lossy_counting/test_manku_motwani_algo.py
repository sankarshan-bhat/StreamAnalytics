from manku_motwani_algo import *
import random
from StreamAnalytics.manku_motwani_lossy_counting.manku_motwani_algo import manku_motwani_algo



####
manku_motwani = manku_motwani_algo(0.01,5)
save_count=[0]*26
save_count_capital = [0]*26
for i in range(0,50000):
    random_count = random.randrange(0,26,1)
    random_decision = random.randrange(0,100,1)
    if(random_decision<90):
        character_count = 97+random_count
        manku_motwani.add(chr(character_count))
        save_count[random_count] = save_count[random_count]+1
    else:
        character_count = 65 + random_count
        manku_motwani.add(chr(character_count))
        save_count_capital[random_count] = save_count_capital[random_count] + 1
for i in range(0,26):
    print(chr(i+97),":",save_count[i])
for i in range(0,26):
    print(chr(i+65),":",save_count_capital[i])

print(manku_motwani.get())
print(manku_motwani.get_with_support(1))
