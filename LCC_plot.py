import cPickle as pickle
import seaborn as sns
import pandas as pd
import numpy as np
import pylab
import matplotlib.patches as mpatches

sns.set_style('whitegrid')

benchmark_data = pickle.load(open("result.pickle", 'r'))

callset_id = benchmark_data['callset_id']

#invoke_df = pd.DataFrame(benchmark_data['results'])
results_df = pd.DataFrame(benchmark_data['run_statuses'])

#results_df = pd.concat([results_df, invoke_df], axis=1)

results_df['runtime_cached'].value_counts()

time_offset = np.min(results_df.host_submit_time)
fig = pylab.figure(figsize=(10, 6))
ax = fig.add_subplot(1, 1, 1)
total_jobs = len(results_df)

y = np.arange(total_jobs)
point_size = 5
ax.scatter(results_df.host_submit_time - time_offset, y, c='b', edgecolor='none', s=point_size)
ax.scatter(results_df.start_time - time_offset, y, c='g', edgecolor='none', s=point_size)
ax.scatter(results_df.end_time - time_offset, y, c='r', edgecolor='none', s=point_size)
ax.scatter(results_df.start_time + results_df.setup_time - time_offset,
           y, c='k', edgecolor='none', s=point_size)
ax.set_xlabel('wallclock time (sec)')
ax.set_ylabel('job')
# pylab.ylim(0, 10)

host_submit_patch = mpatches.Patch(color='b', label='host_submit')
job_start_patch = mpatches.Patch(color='g', label='job start')
setup_done_patch = mpatches.Patch(color='k', label='setup done')

job_done_patch = mpatches.Patch(color='red', label='job done')

legend = pylab.legend(handles=[host_submit_patch, job_start_patch, setup_done_patch, job_done_patch, ],
                      loc='upper right', frameon=True)
pylab.title("Example plot for LCC")
legend.get_frame().set_facecolor('#FFFFFF')

plot_step = 100  # int(np.min([128, total_jobs/32]))
y_ticks = np.arange(total_jobs // plot_step + 2) * plot_step
ax.set_yticks(y_ticks)
ax.set_ylim(-0.02 * total_jobs, total_jobs * 1.05)

ax.set_xlim(-5, np.max(results_df.end_time - time_offset) * 1.05)
for y in y_ticks:
    ax.axhline(y, c='k', alpha=0.1, linewidth=1)

ax.grid(False)
fig.tight_layout()
fig.savefig("example.png")
