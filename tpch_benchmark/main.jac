"""TPC-H Benchmark for Jac — Entry Point.

Usage:
    jac run main.jac

This will load all TPC-H .tbl files from the sibling tpch/ directory,
build the full graph, and run + verify all 22 TPC-H queries.

Graph hierarchy:
    root -> Region -> Nation -> Supplier -> PartSupp
                             -> Customer -> Order -> LineItem -> PartSupp
    root -> Part -> PartSupp

Walkers Q1–Q22 implement the 22 TPC-H queries.
All spawn on root and report results.
"""

import os;
import sys;
import time;

include loader;
include queries;

with entry {
    # Resolve path to tpch/ data directory (sibling of this project)
    script_dir = os.path.dirname(os.path.abspath(__file__));
    data_dir = os.path.join(script_dir, "..", "tpch_small");
    answers_dir = os.path.join(script_dir, "..", "tpch_small", "answers");

    # Jac walker runtime uses recursive calls internally; with 86K+ nodes we need more stack.
    sys.setrecursionlimit(200000);

    # Import verify_answers from the benchmark directory
    sys.path.insert(0, script_dir);
    import verify_answers;

    if not os.path.isdir(data_dir) {
        print(f"Error: TPC-H data directory not found at {data_dir}");
        print("Please ensure the tpch/ folder with .tbl files is present.");
    } else {
        print(f"TPC-H data directory: {os.path.abspath(data_dir)}");
        print("Starting graph construction...\n");
        load_tpch_graph(data_dir);
        print("\n=== Running TPC-H Queries ===\n");

        walker_classes: list = [
            [1, Q1_PricingSummary],
            [2, Q2_MinCostSupplier],
            [3, Q3_ShippingPriority],
            [4, Q4_OrderPriority],
            [5, Q5_LocalSupplierVolume],
            [6, Q6_ForecastingRevenue],
            [7, Q7_VolumeShipping],
            [8, Q8_NationalMarketShare],
            [9, Q9_ProductTypeProfit],
            [10, Q10_ReturnedItemReporting],
            [11, Q11_ImportantStock],
            [12, Q12_ShippingModes],
            [13, Q13_CustomerDistribution],
            [14, Q14_PromotionEffect],
            [15, Q15_TopSupplier],
            [16, Q16_PartsSupplierRelation],
            [17, Q17_SmallQuantityOrder],
            [18, Q18_LargeVolumeCustomer],
            [19, Q19_DiscountedRevenue],
            [20, Q20_PotentialPartPromotion],
            [21, Q21_SuppliersWhoKeptOrdersWaiting],
            [22, Q22_GlobalSalesOpportunity]
        ];

        all_results: dict = {};
        run_queries_env = os.environ.get("QUERIES", "");
        if run_queries_env != "" {
            run_set = set([int(x.strip()) for x in run_queries_env.split(",")]);
        } else {
            run_set = set([i for i in range(1, 23)]);
        }
        for entry_pair in walker_classes {
            qnum = entry_pair[0];
            if qnum not in run_set {
                continue;
            }
            wclass = entry_pair[1];
            t0 = time.time();
            print(f"Running Q{qnum}...", flush=True);
            w = root spawn wclass();
            elapsed = time.time() - t0;
            if len(w.reports) > 0 {
                result = w.reports[0];
            } else {
                result = [];
            }
            all_results[qnum] = result;
            vr = verify_answers.check_query(qnum, result, answers_dir, verbose=True);
            status = "PASS" if vr["pass"] else "FAIL";
            print(f"  Q{qnum}: {status} ({elapsed:.1f}s)\n", flush=True);
        }

        # Summary
        passed = 0;
        failed = 0;
        for qnum in all_results {
            vr = verify_answers.check_query(qnum, all_results[qnum], answers_dir);
            if vr["pass"] {
                passed += 1;
            } else {
                failed += 1;
            }
        }
        total = passed + failed;
        print(f"\n{'=' * 50}");
        print(f"Summary: {passed}/{total} queries passed, {failed} failed");
        print(f"{'=' * 50}");
    }
}
