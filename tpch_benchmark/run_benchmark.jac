"""TPC-H Benchmark: Full Pipeline Runner.

Usage:
    jac run run_benchmark.jac

Steps:
    1. Load all TPC-H .tbl data into the graph.
    2. Spawn all 22 query walkers on root and capture reports.
    3. Run Python verification against expected answer files.
"""

import os;
import time;
import sys;

include loader;
include queries;

with entry {
    # ── Resolve paths ──
    script_dir = os.path.dirname(os.path.abspath(__file__));
    data_dir = os.path.join(script_dir, "..", "tpch");
    answers_dir = os.path.join(data_dir, "answers");

    # Import local Python verifier
    sys.path.insert(0, script_dir);
    import verify_answers;
    check_all_queries = verify_answers.check_all_queries;

    if not os.path.isdir(data_dir) {
        print(f"Error: TPC-H data directory not found at {data_dir}");
        sys.exit(1);
    }
    if not os.path.isdir(answers_dir) {
        print(f"Error: Answers directory not found at {answers_dir}");
        sys.exit(1);
    }

    # ── Step 1: Load the graph ──
    print("=" * 60);
    print("STEP 1: Loading TPC-H graph");
    print("=" * 60);
    load_tpch_graph(data_dir);

    # ── Step 2: Run all 22 query walkers ──
    print("\n" + "=" * 60);
    print("STEP 2: Running 22 TPC-H query walkers");
    print("=" * 60);

    walker_results: dict = {};

    # Q1: Pricing Summary Report
    t0 = time.time();
    print("\n[Q1] Pricing Summary Report...", flush=True);
    w = root spawn Q1_PricingSummary();
    walker_results[1] = w.reports;
    print(f"  Done ({time.time() - t0:.1f}s) — {len(w.reports)} report(s)", flush=True);

    # Q2: Minimum Cost Supplier
    t0 = time.time();
    print("[Q2] Minimum Cost Supplier...", flush=True);
    w = root spawn Q2_MinCostSupplier();
    walker_results[2] = w.reports;
    print(f"  Done ({time.time() - t0:.1f}s) — {len(w.reports)} report(s)", flush=True);

    # Q3: Shipping Priority
    t0 = time.time();
    print("[Q3] Shipping Priority...", flush=True);
    w = root spawn Q3_ShippingPriority();
    walker_results[3] = w.reports;
    print(f"  Done ({time.time() - t0:.1f}s) — {len(w.reports)} report(s)", flush=True);

    # Q4: Order Priority Checking
    t0 = time.time();
    print("[Q4] Order Priority Checking...", flush=True);
    w = root spawn Q4_OrderPriority();
    walker_results[4] = w.reports;
    print(f"  Done ({time.time() - t0:.1f}s) — {len(w.reports)} report(s)", flush=True);

    # Q5: Local Supplier Volume
    t0 = time.time();
    print("[Q5] Local Supplier Volume...", flush=True);
    w = root spawn Q5_LocalSupplierVolume();
    walker_results[5] = w.reports;
    print(f"  Done ({time.time() - t0:.1f}s) — {len(w.reports)} report(s)", flush=True);

    # Q6: Forecasting Revenue Change
    t0 = time.time();
    print("[Q6] Forecasting Revenue Change...", flush=True);
    w = root spawn Q6_ForecastingRevenue();
    walker_results[6] = w.reports;
    print(f"  Done ({time.time() - t0:.1f}s) — {len(w.reports)} report(s)", flush=True);

    # Q7: Volume Shipping
    t0 = time.time();
    print("[Q7] Volume Shipping...", flush=True);
    w = root spawn Q7_VolumeShipping();
    walker_results[7] = w.reports;
    print(f"  Done ({time.time() - t0:.1f}s) — {len(w.reports)} report(s)", flush=True);

    # Q8: National Market Share
    t0 = time.time();
    print("[Q8] National Market Share...", flush=True);
    w = root spawn Q8_NationalMarketShare();
    walker_results[8] = w.reports;
    print(f"  Done ({time.time() - t0:.1f}s) — {len(w.reports)} report(s)", flush=True);

    # Q9: Product Type Profit Measure
    t0 = time.time();
    print("[Q9] Product Type Profit Measure...", flush=True);
    w = root spawn Q9_ProductTypeProfit();
    walker_results[9] = w.reports;
    print(f"  Done ({time.time() - t0:.1f}s) — {len(w.reports)} report(s)", flush=True);

    # Q10: Returned Item Reporting
    t0 = time.time();
    print("[Q10] Returned Item Reporting...", flush=True);
    w = root spawn Q10_ReturnedItemReporting();
    walker_results[10] = w.reports;
    print(f"  Done ({time.time() - t0:.1f}s) — {len(w.reports)} report(s)", flush=True);

    # Q11: Important Stock Identification
    t0 = time.time();
    print("[Q11] Important Stock Identification...", flush=True);
    w = root spawn Q11_ImportantStock();
    walker_results[11] = w.reports;
    print(f"  Done ({time.time() - t0:.1f}s) — {len(w.reports)} report(s)", flush=True);

    # Q12: Shipping Modes and Order Priority
    t0 = time.time();
    print("[Q12] Shipping Modes and Order Priority...", flush=True);
    w = root spawn Q12_ShippingModes();
    walker_results[12] = w.reports;
    print(f"  Done ({time.time() - t0:.1f}s) — {len(w.reports)} report(s)", flush=True);

    # Q13: Customer Distribution
    t0 = time.time();
    print("[Q13] Customer Distribution...", flush=True);
    w = root spawn Q13_CustomerDistribution();
    walker_results[13] = w.reports;
    print(f"  Done ({time.time() - t0:.1f}s) — {len(w.reports)} report(s)", flush=True);

    # Q14: Promotion Effect
    t0 = time.time();
    print("[Q14] Promotion Effect...", flush=True);
    w = root spawn Q14_PromotionEffect();
    walker_results[14] = w.reports;
    print(f"  Done ({time.time() - t0:.1f}s) — {len(w.reports)} report(s)", flush=True);

    # Q15: Top Supplier
    t0 = time.time();
    print("[Q15] Top Supplier...", flush=True);
    w = root spawn Q15_TopSupplier();
    walker_results[15] = w.reports;
    print(f"  Done ({time.time() - t0:.1f}s) — {len(w.reports)} report(s)", flush=True);

    # Q16: Parts/Supplier Relationship
    t0 = time.time();
    print("[Q16] Parts/Supplier Relationship...", flush=True);
    w = root spawn Q16_PartsSupplierRelation();
    walker_results[16] = w.reports;
    print(f"  Done ({time.time() - t0:.1f}s) — {len(w.reports)} report(s)", flush=True);

    # Q17: Small-Quantity-Order Revenue
    t0 = time.time();
    print("[Q17] Small-Quantity-Order Revenue...", flush=True);
    w = root spawn Q17_SmallQuantityOrder();
    walker_results[17] = w.reports;
    print(f"  Done ({time.time() - t0:.1f}s) — {len(w.reports)} report(s)", flush=True);

    # Q18: Large Volume Customer
    t0 = time.time();
    print("[Q18] Large Volume Customer...", flush=True);
    w = root spawn Q18_LargeVolumeCustomer();
    walker_results[18] = w.reports;
    print(f"  Done ({time.time() - t0:.1f}s) — {len(w.reports)} report(s)", flush=True);

    # Q19: Discounted Revenue
    t0 = time.time();
    print("[Q19] Discounted Revenue...", flush=True);
    w = root spawn Q19_DiscountedRevenue();
    walker_results[19] = w.reports;
    print(f"  Done ({time.time() - t0:.1f}s) — {len(w.reports)} report(s)", flush=True);

    # Q20: Potential Part Promotion
    t0 = time.time();
    print("[Q20] Potential Part Promotion...", flush=True);
    w = root spawn Q20_PotentialPartPromotion();
    walker_results[20] = w.reports;
    print(f"  Done ({time.time() - t0:.1f}s) — {len(w.reports)} report(s)", flush=True);

    # Q21: Suppliers Who Kept Orders Waiting
    t0 = time.time();
    print("[Q21] Suppliers Who Kept Orders Waiting...", flush=True);
    w = root spawn Q21_SuppliersWhoKeptOrdersWaiting();
    walker_results[21] = w.reports;
    print(f"  Done ({time.time() - t0:.1f}s) — {len(w.reports)} report(s)", flush=True);

    # Q22: Global Sales Opportunity
    t0 = time.time();
    print("[Q22] Global Sales Opportunity...", flush=True);
    w = root spawn Q22_GlobalSalesOpportunity();
    walker_results[22] = w.reports;
    print(f"  Done ({time.time() - t0:.1f}s) — {len(w.reports)} report(s)", flush=True);

    # ── Step 3: Verify results ──
    print("\n" + "=" * 60);
    print("STEP 3: Verifying results against expected answers");
    print("=" * 60 + "\n");

    summary = check_all_queries(walker_results, answers_dir, verbose=True);

    print(f"\nBenchmark complete: {summary['passed']}/{summary['total']} queries passed.");
}
