"""Test what root spawn Walker() returns."""

walker TestReport {
    has result: dict = {"key": "value"};
    
    can on_root with Root entry {
        report self.result;
    }
}

walker TestNoReport {
    has result: dict = {"key": "value2"};
    
    can on_root with Root entry {
        # No report — just do nothing meaningful
        x = 1;
    }
}

with entry {
    print("=== Test with report ===");
    r1 = root spawn TestReport();
    print(f"Type: {type(r1)}");
    print(f"Value: {r1}");
    
    print("\n=== Test without report ===");
    r2 = root spawn TestNoReport();
    print(f"Type: {type(r2)}");
    print(f"Value: {r2}");
    
    print("\n=== Test access .result on no-report ===");
    if r2 is not None {
        print(f"r2 type: {type(r2)}");
        try {
            print(f"r2[0]: {r2[0]}");
        } except Exception as e {
            print(f"r2[0] error: {e}");
        }
        try {
            print(f"r2.result: {r2.result}");
        } except Exception as e {
            print(f"r2.result error: {e}");
        }
    }
}
