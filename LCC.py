import numpy as np
import time
import pywren
import pickle as pickle
import click
import pandas as pd

def benchmark(workers):
    t1 = time.time()
    N = workers
    D=10

    #iters = [(np.random.rand(D,D) for i in range(3)) for j in range(N)]
    iters = [(np.random.rand(D,D), np.random.rand(D,D), np.random.rand(D,D)) for j in range(N)]

    def f(A):
        a, b, c = A
        return a*b*c

    pwex = pywren.lambda_executor()
    futures = pwex.map(f, iters)

    print("invocation done, dur=", time.time() - t1)
    print("callset id: ", futures[0].callset_id)

    local_jobs_done_timeline = []
    result_count = 0
    while result_count < N:
        fs_dones, fs_notdones = pywren.wait(futures)
        result_count = len(fs_dones)

        local_jobs_done_timeline.append((time.time(), result_count))

        if result_count == N:
            break

        time.sleep(1)
    results = [f.result(throw_except=False) for f in futures]
    run_statuses = [f.run_status for f in futures]
    invoke_statuses = [f.invoke_status for f in futures]

    all_done = time.time()
    total_time = all_done - t1
    print("total time", total_time)
    print results
    print local_jobs_done_timeline

    res = {'total_time': total_time,
           'run_statuses': run_statuses,
           'invoke_statuses': invoke_statuses,
           'callset_id': futures[0].callset_id,
           'local_jobs_done_timeline': local_jobs_done_timeline,
           'results': results}
    return res


def results_to_dataframe(benchmark_data):
    callset_id = benchmark_data['callset_id']

    func_df = pd.DataFrame(benchmark_data['results'])
    statuses_df = pd.DataFrame(benchmark_data['run_statuses'])
    invoke_df = pd.DataFrame(benchmark_data['invoke_statuses'])

    results_df = pd.concat([statuses_df, invoke_df, func_df], axis=1)
    Cols = list(results_df.columns)
    for i, item in enumerate(results_df.columns):
        if item in results_df.columns[:i]: Cols[i] = "toDROP"
    results_df.columns = Cols
    results_df = results_df.drop("toDROP", 1)
    results_df['workers'] = benchmark_data['workers']
    return results_df


@click.command()
@click.option('--workers', default=10, help='how many workers', type=int)
@click.option('--outfile', default='result.pickle',
              help='filename to save results in')
def run_benchmark(workers, outfile):
    res = benchmark(workers)
    res['workers'] = workers

    pickle.dump(res, open(outfile, 'wb'), -1)


if __name__ == "__main__":
    run_benchmark()
