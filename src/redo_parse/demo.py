import glob
import os
import time
from subprocess import call


arch_dir = '/home/oracle/app/oracle/oradata/orcl/arch'
arch_fromat = '/home/oracle/app/oracle/oradata/orcl/arch/1_%d_850903564.dbf'
last_arch = sorted(glob.glob('%s/1_*_850903564.dbf' % arch_dir))[-1]
last_seq = int(last_arch.split('_')[1])

while True:
    last_seq += 1
    next_arch_file = arch_fromat % last_seq
    while True:
        if os.path.isfile(next_arch_file):
            break
        time.sleep(0.1)
    call(['./worktest', next_arch_file])
