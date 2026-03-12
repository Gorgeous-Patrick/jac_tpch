"""Quick smoke test: load graph, run Q6, verify."""

import os;
import time;
import sys;

include loader;
include queries;

with entry {
    script_dir = os.path.dirname(os.path.abspath(__file__));
    data_dir = os.path.join(script_dir, "..", "tpch");
    answers_dir = os.path.join(data_dir, "answers");

    sys.path.insert(0, script_dir);
    import verify_answers;

    print("Loading graph...");
    load_tpch_graph(data_dir);

    print("\nRunning Q6...");
    t0 = time.time();
    w = root spawn Q6_ForecastingRevenue();
    print(f"  Done ({time.time() - t0:.1f}s)");
    print(f"  Reports: {w.reports}");

    print("\nVerifying Q6...");
    result = verify_answers.check_query(6, w.reports, answers_dir, verbose=True);
    print(f"  Pass: {result['pass']}");
}
