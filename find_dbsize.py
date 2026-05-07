import subprocess, os

# Find any db files and check their sizes
import glob as g
for p in g.glob('D:/VatsalFiles/PricingModule/**/*.db', recursive=True):
    size = os.path.getsize(p)
    print(f'{size}: {p}')